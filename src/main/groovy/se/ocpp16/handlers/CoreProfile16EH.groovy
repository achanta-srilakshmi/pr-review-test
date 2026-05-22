package se.ocpp16.handlers

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.PromiseFulfiller
import eu.chargetime.ocpp.SessionEvents
import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler
import eu.chargetime.ocpp.model.Confirmation
import eu.chargetime.ocpp.model.Request
import eu.chargetime.ocpp.model.core.*
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import jakarta.inject.Inject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.OCPPEventTypes
import se.service.ConnectionService
import se.service.IdTagCacheService

import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture

/**
 * Implementation class for Core Profile of OCPP
 */
public class CoreProfile16EH implements ServerCoreEventHandler, PromiseFulfiller {
  private static long txnID = System.currentTimeMillis() % (1000000000)

  private static final Logger logger = LoggerFactory.getLogger(CoreProfile16EH.class)

  EdgeConfig edgeConfig

  OcppEventReceiver ocppIncoming

  ObjectMapper objectMapper

  MeterRegistry meterRegistry

  ConnectionService connections

  boolean kafkaEnabled = true

  MessageProducer messageProducer

  IdTagCacheService idTagCacheService

  public CoreProfile16EH() {
    logger.info("Initializing CoreProfile16EH with {}", this.properties)
  }

  @Override
  public AuthorizeConfirmation handleAuthorizeRequest(UUID sessionIndex, AuthorizeRequest request) {

    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new AuthorizeConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Invalid))
    }

    ProtocolLog.debug(true, identifier + "::IDTAG::${request.idTag}", request?.toString())
    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request?.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request?.class.simpleName], 1)

    if (edgeConfig.cacheValidation.idTagCheck) {
      if (!idTagCacheService.isCacheLoaded() || !idTagCacheService.isValid(request.idTag)) {
        logger.warn("idTag {} not in cache, rejecting Authorize for {}", request.idTag, identifier)
        return new AuthorizeConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Invalid))
      }
    }

    assert edgeConfig.kafkaNodeId != null
    def authPayload = [tokenId: request.idTag, tokenType: "RFID", identifier: identifier]

    def isAuthorized = false
    try {
      HttpClient dmClient = HttpClient.create(edgeConfig.authServerUrl.toURL())
      HttpRequest dmRequest = HttpRequest.POST(edgeConfig.authUri, authPayload)
      logger.debug("Incoming \n" + request)
      HttpResponse<String> response = dmClient.toBlocking().exchange(dmRequest, String)
      logger.info("Authorization response: ${response.body()}")
      def authResponse = objectMapper.readValue(response.body(), Map.class)
      if (response.status == HttpStatus.OK) {
        isAuthorized = authResponse?.data?.authorize
        def json = objectMapper.writeValueAsString([tokenId: request.idTag, isAuthorized: isAuthorized, reason: authResponse?.message])
        this.ocppIncoming.authorize(edgeConfig.kafkaNodeId, identifier, json)
      } else {
        logger.error("Error processing request to Authorize transaction")
        def json = objectMapper.writeValueAsString([tokenId: request?.idTag, isAuthorized: false, reason: authResponse?.message])
        this.ocppIncoming.authorize(edgeConfig.kafkaNodeId, identifier, json)
      }
    } catch (Exception e) {
      logger.error("Error while delegating call to authorize ", e)
      def json = objectMapper.writeValueAsString([tokenId: request?.idTag, isAuthorized: false, reason: "Error"])
      this.ocppIncoming.authorize(edgeConfig.kafkaNodeId, identifier, json)
    }

    if (edgeConfig.authBypass) {
      logger.warn("*******************************************************************************")
      logger.warn("Server side authorization for this transaction BYPASSED via configuration in SE")
      logger.warn("*******************************************************************************")
    }

    def response = new AuthorizeConfirmation(idTagInfo: new IdTagInfo(status: (edgeConfig.authBypass) ? AuthorizationStatus.Accepted :
        (isAuthorized ? AuthorizationStatus.Accepted : AuthorizationStatus.Blocked)))


    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  public BootNotificationConfirmation handleBootNotificationRequest(UUID sessionIndex, BootNotificationRequest request) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new BootNotificationConfirmation(currentTime: ZonedDateTime.now(), interval: 0, status: RegistrationStatus.Rejected)
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)


    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())
    if (kafkaEnabled) {
      def requestMap = objectMapper.convertValue(request, Map.class)
      requestMap.put("timestamp", timeNow)
      def json = objectMapper.writeValueAsString(requestMap)
      logger.debug("BootNotification timestamp: ${timeNow}")
      //this.ocppIncoming.bootCycle(edgeConfig.kafkaNodeId, sessionIndex.toString(), identifier, OCPPEventTypes.BOOT_NOTIFICATION.name, json)
      this.ocppIncoming.bootNotification(edgeConfig.kafkaNodeId, sessionIndex.toString(), identifier, json)
    }

    BootNotificationConfirmation conf = new BootNotificationConfirmation(currentTime: timeNow, interval: edgeConfig.chargers.heartbeatIntervalInSec, status: RegistrationStatus.Accepted)


    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, conf?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(conf))

    return conf
  }

  @Override
  public DataTransferConfirmation handleDataTransferRequest(UUID sessionIndex, DataTransferRequest request) {

      Integer messageTypeId = 2
      String uniqueId = System.currentTimeMillis().toString()
    def identifier = connections.getChargerId(sessionIndex.toString())
      ProtocolLog.info(true, identifier,messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))


    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new DataTransferConfirmation(DataTransferStatus.Rejected)
    }

    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    if (kafkaEnabled)
      this.ocppIncoming.dataTransfer(edgeConfig.kafkaNodeId, identifier, objectMapper.writeValueAsString(request))
    // Since we are not handling DT in any other way now

    def response = new DataTransferConfirmation(DataTransferStatus.Accepted)

      messageTypeId = 3
      ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  public HeartbeatConfirmation handleHeartbeatRequest(UUID sessionIndex, HeartbeatRequest request) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new HeartbeatConfirmation(currentTime: ZonedDateTime.now())
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    if (kafkaEnabled)
      this.ocppIncoming.heartbeat(edgeConfig.kafkaNodeId, identifier, "{}")
    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())

    def response = new HeartbeatConfirmation(timeNow)

    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))
    return response
  }

  @Override
  public MeterValuesConfirmation handleMeterValuesRequest(UUID sessionIndex, MeterValuesRequest request) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new MeterValuesConfirmation()
    }

    //iterate through the list of meter values and generate json string
    def requestString = ""
    request.meterValue?.each { meterValue ->
      requestString += objectMapper.writeValueAsString(meterValue)
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    if (kafkaEnabled) {
      def json = objectMapper.writeValueAsString(request)
      this.ocppIncoming.chargerMeterValues(edgeConfig.kafkaNodeId, identifier,"UNKNOWN", json)
    }

    def response = new MeterValuesConfirmation()

    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  public StartTransactionConfirmation handleStartTransactionRequest(UUID sessionIndex, StartTransactionRequest request) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Invalid), 0)
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    Integer transactionId = System.currentTimeMillis() % (1000000000)

    if (edgeConfig.cacheValidation.idTagCheck) {
      if (!idTagCacheService.isCacheLoaded() || !idTagCacheService.isValid(request.idTag)) {
        logger.warn("StartTransaction rejected: idTag {} not valid for {}", request.idTag, identifier)
        return new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Invalid), transactionId)
      }
    }

    def evokePayload = [:]
    request.properties.each { evokePayload.put(it.key, it.value) }
    evokePayload.put("seTransactionId", transactionId)

    if (kafkaEnabled) {
      def json = objectMapper.writeValueAsString(evokePayload)
      this.ocppIncoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.START_TRANSACTION.name, "UNKNOWN", json)
    }

    def response = new StartTransactionConfirmation(new IdTagInfo(AuthorizationStatus.Accepted), transactionId)

    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))


    return response
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotificationRequest(UUID sessionIndex, StatusNotificationRequest request) {

    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new StatusNotificationConfirmation()
    }

    if (request.errorCode != null && request.errorCode != ChargePointErrorCode.NoError) {
      meterRegistry.counter("se.in", "identifier", identifier, "StatusError", request.errorCode as String).increment()
      publishMetricsCounterIncrement("se.in", ["identifier": identifier, "StatusError": request.errorCode as String], 1)
      logger.warn("StatusNotificationRequest error code: ${request.errorCode}, vendorErrorCode: ${request.vendorErrorCode}")
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)


    if (kafkaEnabled) {
      def json = objectMapper.writeValueAsString(request)
      this.ocppIncoming.bootCycle(edgeConfig.kafkaNodeId, sessionIndex.toString(), identifier, OCPPEventTypes.STATUS_NOTIFICATION.name, json)
    }

    def response = new StatusNotificationConfirmation()

      messageTypeId = 3
      ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  public StopTransactionConfirmation handleStopTransactionRequest(UUID sessionIndex, StopTransactionRequest request) {
    def identifier = connections.getChargerId(sessionIndex.toString())
    //null handling for identifier
    if (identifier == null) {
      logger.error("No charger found for sessionIndex: {}", sessionIndex)
      return new StopTransactionConfirmation()
    }

    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))

    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    if(kafkaEnabled){
      def json = objectMapper.writeValueAsString(request)
      this.ocppIncoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.STOP_TRANSACTION.name, "UNKNOWN", json)
    }

    def response = new StopTransactionConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Accepted))
    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  void fulfill(CompletableFuture<Confirmation> promise, SessionEvents eventHandler, Request request) {
    logger.debug(request.toString())
  }

  private <T> Mono<T> send(final Callable<T> callable) {
    return Mono.fromCallable(callable)
        .subscribeOn((Schedulers.boundedElastic()));
  }

  private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
    def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
    if(kafkaEnabled)
      messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
  }
}
