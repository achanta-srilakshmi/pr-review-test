package se.ocpp16

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.AuthenticationException
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.model.SessionInformation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageConfirmation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageStatus
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Inject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.ProtocolLog
import se.ocpp16.handlers.SessionAuthenticator
import se.service.ChargerIdentifierCacheService
import se.service.ConnectionService

import java.time.ZoneId
import java.time.ZonedDateTime

class SocketSessionEvents implements ServerEvents {
  private static final Logger logger = LoggerFactory.getLogger(SocketSessionEvents.class)

  private int count

  ObjectMapper objectMapper

  OcppEventReceiver ocppIncoming

  MeterRegistry meterRegistry

  ConnectionService connections

  long startupTime

  EdgeConfig edgeConfig

  public static final WS_MSG_WAIT_PERIOD = 30

  boolean kafkaEnabled = true

  SessionAuthenticator authenticator

  MessageProducer messageProducer

  ChargerIdentifierCacheService chargerIdentifierCacheService

  /**
   * ThreadLocal fallback to carry the charger identifier from authenticateSession() to lostSession()
   * when auth fails before newSession() registers the session in ConnectionService.
   * Always cleaned up in both newSession() (success) and lostSession() (failure) paths.
   */
  private static final ThreadLocal<String> pendingAuthIdentifier = new ThreadLocal<>()

  SocketSessionEvents(ObjectMapper objectMapper,
                      OcppEventReceiver ocppIncoming,
                      MeterRegistry meterRegistry,
                      ConnectionService connections,
                      EdgeConfig edgeConfig,
                      SessionAuthenticator authenticator,
                      boolean enableKafka,
                      MessageProducer messageProducer,
                      ChargerIdentifierCacheService chargerIdentifierCacheService
  ) {
    this.objectMapper = objectMapper
    this.ocppIncoming = ocppIncoming
    this.meterRegistry = meterRegistry
    this.connections = connections
    this.edgeConfig = edgeConfig
    this.authenticator = authenticator
    this.kafkaEnabled = enableKafka
    this.messageProducer = messageProducer
    this.chargerIdentifierCacheService = chargerIdentifierCacheService
  }

  def selfcheck() {
    assert objectMapper
    assert ocppIncoming
    assert meterRegistry
    logger.info("Self check success")
  }

  @Override
  void authenticateSession(SessionInformation information, String username, byte[] password) throws AuthenticationException {
    String evseIdentifier = this.deduceIdentifier(information)
    pendingAuthIdentifier.set(evseIdentifier) // Store for lostSession fallback if auth fails before newSession
    String profile = edgeConfig.connectionProfile

    if (edgeConfig.authorizedSocket.enabled) {
      if (!authenticator.authenticate(evseIdentifier, username, password)) {
        throw new AuthenticationException(199, "Invalid credentials. The charger ${evseIdentifier} is not allowed to connect.")
      } else {
        profile = profile+"_AUTH"
      }
    }
    def sessionId = connections.getLatestSessionId(evseIdentifier)
    if (kafkaEnabled) {
      ProtocolLog.debug(true, evseIdentifier, objectMapper.writeValueAsString([identifier: evseIdentifier, profile: profile, eventType: OCPPEventTypes.STATION_AUTH.name]))
      ocppIncoming.bootCycle(
        edgeConfig.kafkaNodeId,
        sessionId.toString(),
        evseIdentifier,
        OCPPEventTypes.STATION_AUTH.name,
        objectMapper.writeValueAsString([identifier: evseIdentifier, profile: profile])
      )
    }
  }

  @Override
  void newSession(UUID sessionIndex, SessionInformation information) {
    String evseIdentifier = this.deduceIdentifier(information)
    pendingAuthIdentifier.remove() // Clean up — auth succeeded, no longer needed

    // US-SE-CACHE108-02: Charger identifier cache gate — close(1008) unregistered chargers
    // Gate is skipped entirely when chargerIdCheck=false (default) — zero impact on existing flow
    if (edgeConfig.cacheValidation.chargerIdCheck
        && chargerIdentifierCacheService.isCacheLoaded()
        && !chargerIdentifierCacheService.isAuthorized(evseIdentifier)) {
      logger.warn("Charger {} is not in the authorized charger cache — closing connection (1008)", evseIdentifier)
      ProtocolLog.debug(false, evseIdentifier, "Charger not in authorized cache — closing connection (1008)")
      pendingAuthIdentifier.set(evseIdentifier) // Re-set so lostSession can identify the charger
      connections.closeSession(sessionIndex)
      return
    }

    logger.info("Opening websocket for $evseIdentifier with sessionId ${sessionIndex.toString()} on ${edgeConfig.kafkaNodeId}")
    ProtocolLog.debug(true, evseIdentifier, "Opening websocket "+objectMapper.writeValueAsString(information))
    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())
    logger.debug("NewSession timestamp: ${timeNow}")
    String profile = edgeConfig.connectionProfile
    if (kafkaEnabled)
      ocppIncoming.bootCycle(edgeConfig.kafkaNodeId, sessionIndex.toString(), evseIdentifier, OCPPEventTypes.NEW_SESSION.name, objectMapper.writeValueAsString([identifier: evseIdentifier, timestamp: timeNow, "profile":profile]))

    connections.newConnection(sessionIndex.toString(), evseIdentifier)

    // US-CERT106-03: Detect successful reconnection after security profile upgrade (TC-64)
    def completedUpgrade = connections.completeSecurityProfileUpgrade(evseIdentifier)
    if (completedUpgrade) {
      logger.info("CP {} successfully reconnected after security profile upgrade to Profile {}", evseIdentifier, completedUpgrade.targetProfile)
    }

    meterRegistry.counter("sessions.new", "identifier", evseIdentifier).increment()
    publishMetricsCounterIncrement("sessions.new", ["identifier": evseIdentifier], 1)

    logger.trace("refreshOnStartup: ${edgeConfig.chargers.updateOnNewConnection}")
    //Request Status for every new session
    if (edgeConfig.chargers.updateOnNewConnection) {
      requestStatus(sessionIndex, 1000)
    }
  }

  private void requestStatus(UUID sessionIndex, int delay) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    logger.trace("Refreshing status on new connection")
    def thread = Thread.start {
      TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.StatusNotification)
      if (delay > 0) Thread.sleep(delay)
      def msgConfirmation = (TriggerMessageConfirmation) connections.callInline(sessionIndex, request)
      meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
      publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)
      if (msgConfirmation?.status != TriggerMessageStatus.Accepted) {
        meterRegistry.counter("se.err", "type", "Status after new session failed")
        publishMetricsCounterIncrement("se.err", ["type": "Status after new session failed"], 1)
        if(kafkaEnabled)
          ocppIncoming.chargerError(edgeConfig.kafkaNodeId, identifier, TriggerMessageRequestType.StatusNotification.toString() + "Error", objectMapper.writeValueAsString(msgConfirmation))
      }
    }
  }

  @Override
  public void lostSession(UUID sessionIndex) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    if (identifier == null) {
      identifier = pendingAuthIdentifier.get()
    }
    pendingAuthIdentifier.remove() // Always clean up to prevent stale data on thread reuse

    logger.info("Lost session {} for {}", sessionIndex.toString(), identifier ?: "UNKNOWN")

    // US-CERT106-03: Suppress LostSession during security profile upgrade grace period (TC-64)
    if (identifier && connections.isUpgradingSecurityProfile(identifier)) {
      logger.info("CP {} disconnected for security profile upgrade — suppressing LostSession (grace period active)", identifier)
      return
    }

    def reason = ["message": "SE received lost session event for " + (identifier ?: "UNKNOWN")]
    ProtocolLog.debug(false, identifier ?: "UNKNOWN", "Socket session closed, "+reason)
    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())
    logger.debug("LostSession timestamp: ${timeNow}")
    if (kafkaEnabled)
      ocppIncoming.inactiveCharger(edgeConfig.kafkaNodeId, identifier ?: "UNKNOWN", OCPPEventTypes.INACTIVE_CHARGER.name, objectMapper.writeValueAsString([timestamp: timeNow]))

    meterRegistry.counter("sessions.lost", "identifier", identifier?:"UNKNOWN").increment()
    publishMetricsCounterIncrement("sessions.lost", ["identifier": identifier?:"UNKNOWN"], 1)
    connections.lostConnection(sessionIndex.toString())
  }

  private String deduceIdentifier(SessionInformation si) {
    def parts = si.identifier?.split('/')
    return (parts.length > 0) ? parts.last() : null;
  }

  private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
    def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
    if (kafkaEnabled)
      messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
  }
}
