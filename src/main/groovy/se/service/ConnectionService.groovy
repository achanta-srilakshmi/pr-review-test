package se.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.Confirmation
import eu.chargetime.ocpp.model.Request
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType
import grails.gorm.transactions.Transactional
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Value
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import se.EdgeConfig
import se.bus.MessageProducer
import se.domain.SocketConnection
import se.ocpp16.SocketSessionEvents
import se.ocpp16.handlers.ProtocolLog

import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

@Singleton
class ConnectionService {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionService.class)

  /**
   * Tracks a pending security profile upgrade for expected-disconnect suppression.
   * @since US-CERT106-03 (TC-64)
   */
  static class SecurityProfileUpgrade {
    int targetProfile
    Instant timestamp
    SecurityProfileUpgrade(int targetProfile, Instant timestamp) {
      this.targetProfile = targetProfile
      this.timestamp = timestamp
    }
  }

  /**
   * Map of charger ID → pending security profile upgrade state.
   * Used to suppress LostSession during expected disconnection after
   * ChangeConfiguration(SecurityProfile) is accepted by the charge point.
   * @since US-CERT106-03 (TC-64)
   */
  final Map<String, SecurityProfileUpgrade> pendingUpgrades = new ConcurrentHashMap<>()

  @Inject
  EdgeConfig edgeConfig

  @Inject
  AtomicInteger sessionGauge

  JSONServer jsonServer

  /** WSS server reference — set by CoordinatingFactory when WSS is enabled */
  JSONServer wssJsonServer

  @Inject
  MeterRegistry meterRegistry

  @Inject
  MessageProducer messageProducer

  @Inject
  ObjectMapper objectMapper

  boolean kafkaEnabled = true

  /**
   * ConcurrentSkipListMap to store the latest websocket session id for each charger
   */
  def chargerMap = new ConcurrentSkipListMap<String, String>()

  // New cache with a time-based eviction policy of 1 hour
  def sessionMap = CacheBuilder.newBuilder()
      .expireAfterAccess(24, TimeUnit.HOURS)
      .build(new CacheLoader<String, String>() {
        @Override
        public String load(String sessionId) {
          return null
        }
      })

  /**
   * Function to add a new websocket session
   */
  def addSession(chargerId, sessionId) {
    logger.debug("Adding {} against {}", sessionId, chargerId)
    if (chargerMap.containsKey(chargerId)) {
      def oldSessionId = chargerMap.get(chargerId)
      logger.trace("Removing old session {} for {}", oldSessionId, chargerId)
      def oldSessionUUID = UUID.fromString(oldSessionId)
      def srv = serverForSession(oldSessionUUID)
      if (srv.isSessionOpen(oldSessionUUID)) {
        logger.debug("Session cleanup: closing session {} on {}", oldSessionId, chargerId)
        srv.closeSession(oldSessionUUID)
      } else {
        logger.trace("Session {} is already closed for {}", oldSessionId, chargerId)
      }
      sessionMap.invalidate(oldSessionId)
    }
    synchronized(this){
      chargerMap.put(chargerId, sessionId)
      sessionMap.put(sessionId, chargerId)
    }
  }

  // Function to retrieve the latest websocket session id for a specific charger
  def getLatestSessionId(chargerId) {
    return chargerMap.get(chargerId)
  }

  // Function to retrieve the charger id for a specific session id
  def getChargerId(sessionId) {
    return sessionMap.getIfPresent(sessionId)
  }

  def newConnection(String sessionId, String identifier) {
    addSession(identifier, sessionId)
    logger.debug("Total active sessions: {}", chargerMap.size())
  }

  def lostConnection(String sessionId) {
    try {
      removeSession(sessionId)
    } catch (Exception e) {
      logger.warn("Error while cleaning up session", e)
    }
  }

  @Scheduled(fixedRate = '${se.config.chargers.syncFrequency:6h}', initialDelay = '90s')
  def refreshChargerMap() {
    logger.info("refreshChargerMap called on {}", new Date())
    int updateCount = 0
    synchronized(this) {
      sessionMap.asMap().each { sessionId, chargerId ->
        if (!chargerMap.containsKey(chargerId)) {
          if (isSessionOpenOnAnyServer(UUID.fromString(sessionId))) {
            logger.warn("Adding the missing chargerId: $chargerId against sessionId: $sessionId in the chargerMap")
            chargerMap.put(chargerId, sessionId)
            updateCount++
          }
        }

        //review the entries in chargerMap and print all those chargers which are not in sessionMap
        chargerMap.each { chargerId1, sessionId1 ->
          if (!sessionMap.asMap().containsKey(sessionId1)) {
            logger.warn("ChargerId: $chargerId1 is not in sessionMap")
            if (isSessionOpenOnAnyServer(UUID.fromString(sessionId))) {
              logger.warn("Adding the missing sessionId: $sessionId1 against chargerId: $chargerId1 in the sessionMap")
              sessionMap.put(sessionId1, chargerId1)
              updateCount++
            }
          }
        }
      }
    }
    logger.debug("Charger map updated {} times", updateCount)
    return updateCount
  }

  // Function to remove a websocket session
  def removeSession(sessionId) {
    def chargerId = sessionMap.getIfPresent(sessionId)
    if (chargerId != null) {
      synchronized (this){
        if(!isSessionOpenOnAnyServer(UUID.fromString(sessionId))){
          chargerMap.remove(chargerId)
          sessionMap.invalidate(sessionId)
        }
      }
    }
  }

  //@Scheduled(cron = "0 */2 * * *", initialDelay = "900s")
  @Transactional
  synchronized void clearStaleConnectionData() {
    logger.info("Clearing stale connection data from the Edge Database")
    try {
      def c = SocketConnection.createCriteria()
      def connections = c.list {
        eq("isActive", false)
      }
      connections?.each { SocketConnection connection ->
        SocketConnection.withTransaction {
          connection.delete()
        }
      }
    } catch (Exception e) {
      logger.error(e)
    }
  }

  //@Scheduled(fixedRate = '${se.config.chargers.statusUpdateFrequency:10m}', initialDelay = '30s')
  void refreshStatuses() {
    logger.info("******************************************")
    logger.info("Refreshing statuses of all online chargers: ${chargerMap.size()}")
    logger.info("******************************************")

    chargerMap.each { String chargerId, String sessionId ->
      //logger.trace("Requesting status for {}", chargerId)
      TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.StatusNotification)
      this.callInline(UUID.fromString(sessionId), request)
    }
  }

  @Scheduled(fixedRate = '5m', initialDelay = '90s')
  void trackActiveCount() {
    sessionGauge.set(jsonServer?.server?.sessions?.size() ?: 0)
    logger.debug("Total chargers: {}", chargerMap.size())
    logger.debug("Total sessions: {}", sessionMap.size())
  }

  Confirmation callInline(UUID sessionId, Request request) {
    def identifier = this.getChargerId(sessionId.toString())
    try {
      def start = System.currentTimeMillis()

      Integer messageTypeId = 2
      String uniqueId = start.toString()
      ProtocolLog.info(false, identifier,messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
      meterRegistry.counter("se.out", "type", request.class.simpleName).increment()
      publishMetricsCounterIncrement("se.out", ["type": request.class.simpleName], 1)

      CompletionStage<Confirmation> cs = serverForSession(sessionId).send(sessionId, request)
      def confirmation = cs.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)

      def end = System.currentTimeMillis()
      long latency = end - start // Calculate latency

      meterRegistry.timer("se.latency", "identifier", identifier, "request", request?.class?.simpleName).record(Duration.ofMillis(latency)) // Record latency in metrics
      //logger.trace("Duration of ${request.class.simpleName}: ${end - start}")
      messageTypeId = 3
      ProtocolLog.info(true, identifier, messageTypeId, uniqueId, confirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(confirmation))

      return confirmation
    } catch (NotConnectedException nce) {
      logger.warn("Probable case of incorrect SE instance")
    } catch (TimeoutException te) {
      logger.error("Request to charger timed out for ${request.class.simpleName}")
      meterRegistry.counter("se.ws.out.error", "identifier", identifier, "errorType", "REQUEST_TIMEDOUT", "requestType", request.class.simpleName).increment()
      publishMetricsCounterIncrement("se.ws.out.error", ["identifier": identifier, "errorType": "REQUEST_TIMEDOUT", "requestType": request.class.simpleName], 1)
    } catch (InterruptedException te) {
      logger.error("Request to charger interrupted for ${request.class.simpleName}")
      meterRegistry.counter("se.ws.out.error", "identifier", identifier, "errorType", "REQUEST_INTERRUPTED", "requestType", request.class.simpleName).increment()
      publishMetricsCounterIncrement("se.ws.out.error", ["identifier": identifier, "errorType": "REQUEST_INTERRUPTED", "requestType": request.class.simpleName], 1)
    } catch (Exception e) {
      logger.error("Exception while forwarding message to SE", e)
    }
  }

  public <Req> void pushCommandWithDelay(UUID sessionIndex, Req request, Closure successHandler, long delay, String uniqueId) {
    /**
     * The callable closure fulfills the responsibility of submitting the request to the charger.
     * Appropriate error handling ensures the websocket connection is handled as expected
     */
    def callable = () -> {
      if (delay > 0)
        Thread.sleep(delay)

        Integer messageTypeId = 2
        ProtocolLog.info(false, this.getChargerId(sessionIndex.toString()),messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))

        try {
        meterRegistry.counter("se.out", "type", request.class.simpleName).increment()
        publishMetricsCounterIncrement("se.out", ["type": request.class.simpleName], 1)
        serverForSession(sessionIndex).send(sessionIndex, request)
      } catch (NotConnectedException nce) {
        // NCE is reported at trace level
        logger.trace("Probable wrong instance")
      } catch (Exception ex) {
        logger.error(ex.getMessage(), ex)
        throw ex
      }
    }

    def error = (err) -> {
      logger.error("${request?.class?.simpleName} failed with error", err)
    }

    Mono.fromCallable(callable)
        .subscribeOn(Schedulers.immediate())
        .subscribe(successHandler, error)
  }

  def void closeSession(UUID sessionId) {
    try {
      serverForSession(sessionId).closeSession(sessionId)
      logger.trace("Closed session with ID " + sessionId + " on User's request")
    } catch (NotConnectedException nce) {
      logger.trace("Probable wrong instance")
    } catch (Exception e) {
      logger.error("Error while closing session", e)
    }
  }

  /**
   * Marks a charger as undergoing a security profile upgrade.
   * Suppresses LostSession events during the grace period.
   * @since US-CERT106-03 (TC-64)
   */
  void markSecurityProfileUpgrade(String chargerId, int targetProfile) {
    pendingUpgrades.put(chargerId, new SecurityProfileUpgrade(targetProfile, Instant.now()))
    logger.info("Marked {} for security profile upgrade to Profile {}", chargerId, targetProfile)
  }

  /**
   * Checks if a charger is currently upgrading its security profile.
   * Returns true if an upgrade is in progress and within the grace period.
   * Returns false and removes the entry if the grace period has expired.
   * @since US-CERT106-03 (TC-64)
   */
  boolean isUpgradingSecurityProfile(String chargerId) {
    def upgrade = pendingUpgrades.get(chargerId)
    if (!upgrade) return false
    if (Duration.between(upgrade.timestamp, Instant.now()).seconds < edgeConfig.mtls.upgradeGracePeriodSeconds) {
      return true
    }
    pendingUpgrades.remove(chargerId)
    return false
  }

  /**
   * Completes and removes a pending security profile upgrade for a charger.
   * Called when the charger successfully reconnects after the upgrade.
   * @return the upgrade info, or null if no pending upgrade exists
   * @since US-CERT106-03 (TC-64)
   */
  SecurityProfileUpgrade completeSecurityProfileUpgrade(String chargerId) {
    return pendingUpgrades.remove(chargerId)
  }

  private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
    def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
    if (kafkaEnabled)
      messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
  }

  /**
   * Returns the JSONServer that holds the given session — checks both WS and WSS servers.
   * Falls back to the WS jsonServer if neither reports the session as open.
   */
  private JSONServer serverForSession(UUID sessionId) {
    if (wssJsonServer != null) {
      try {
        if (wssJsonServer.isSessionOpen(sessionId)) return wssJsonServer
      } catch (Exception ignored) {}
    }
    return jsonServer
  }

  /**
   * Checks if a session is open on either WS or WSS server.
   */
  private boolean isSessionOpenOnAnyServer(UUID sessionId) {
    if (jsonServer.isSessionOpen(sessionId)) return true
    if (wssJsonServer != null) {
      try {
        if (wssJsonServer.isSessionOpen(sessionId)) return true
      } catch (Exception ignored) {}
    }
    return false
  }
}
