package se.bus

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.core.*
import eu.chargetime.ocpp.model.firmware.GetDiagnosticsConfirmation
import eu.chargetime.ocpp.model.firmware.GetDiagnosticsRequest
import eu.chargetime.ocpp.model.firmware.UpdateFirmwareConfirmation
import eu.chargetime.ocpp.model.firmware.UpdateFirmwareRequest
import eu.chargetime.ocpp.model.localauthlist.*
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageConfirmation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageStatus
import eu.chargetime.ocpp.model.securityext.CertificateSignedConfirmation
import eu.chargetime.ocpp.model.securityext.CertificateSignedRequest
import eu.chargetime.ocpp.model.securityext.DeleteCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.DeleteCertificateRequest
import eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageConfirmation
import eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageRequest
import eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsConfirmation
import eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsRequest
import eu.chargetime.ocpp.model.securityext.InstallCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.InstallCertificateRequest
import eu.chargetime.ocpp.model.securityext.GetLogConfirmation
import eu.chargetime.ocpp.model.securityext.GetLogRequest
import eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareConfirmation
import eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareRequest
import eu.chargetime.ocpp.model.securityext.types.CertificateHashDataType
import eu.chargetime.ocpp.model.securityext.types.CertificateUseEnumType
import eu.chargetime.ocpp.model.securityext.types.FirmwareType
import eu.chargetime.ocpp.model.securityext.types.HashAlgorithmEnumType
import eu.chargetime.ocpp.model.securityext.types.LogEnumType
import eu.chargetime.ocpp.model.securityext.types.LogParametersType
import eu.chargetime.ocpp.model.securityext.types.MessageTriggerEnumType
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageHeader
import io.micronaut.scheduling.annotation.Async
import jakarta.inject.Inject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.scheduler.Scheduler
import se.EdgeConfig
import se.ocpp16.OCPPEventTypes
import se.ocpp16.SocketSessionEvents
import se.ocpp16.handlers.ProtocolLog
import se.service.AuthCacheService
import se.service.ChargingProfileService
import se.service.ConnectionService
import se.service.ReservationService


import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

/**
 * Responsible for forwarding Ocpp Messages from the Evoke Bus to the Charger
 *
 */
@KafkaListener(groupId = "se", uniqueGroupId = true,
    errorStrategy = @ErrorStrategy(
        value = ErrorStrategyValue.RESUME_AT_NEXT_RECORD,
        retryDelay = '500ms',
        retryCount = 3
))
@Requires(property = "kafka.enabled", value = "true")
class OcppEventBroadcaster {
    private static final Logger logger = LoggerFactory.getLogger(OcppEventBroadcaster.class)

    @Inject
    EdgeConfig edgeConfig

    @Inject
    ObjectMapper objectMapper

    @Inject
    OcppEventReceiver incoming

    @Inject
    MeterRegistry meterRegistry

    @Inject
    Scheduler schedulerType

    @Inject
    ConnectionService connections

    @Inject
    ChargingProfileService chargingProfileService

    @Inject
    ReservationService reservationService

    @Inject
    AuthCacheService authCacheService

    @Inject
    MessageProducer messageProducer

    boolean kafkaEnabled = true

    /**
     * On the event of a new Socket connection, SE will request the Charger for a boot notification
     */
    @Topic("TriggerBootRequest")
    public void requestBoot(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, @MessageHeader("eventType") String eventType, String payload) {
        logger.debug("Processing {} for the charger {} and eventType {}", "requestBoot", identifier, eventType)
        try {
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            } else {

                TriggerMessageRequest bootRequest = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.BootNotification)

                if (eventType.equalsIgnoreCase("TriggerBootRequest")) {
                    def msgConfirmation = (TriggerMessageConfirmation) connections.callInline(UUID.fromString(sessionId), bootRequest)
                    meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)
                    if (msgConfirmation?.status != TriggerMessageStatus.Accepted) {
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "BootRequestError", objectMapper.writeValueAsString(msgConfirmation))
                    } else
                        logger.debug(msgConfirmation.toString())
                }
            }
        } catch (NotConnectedException nce) {
            //NCE is reported at trace level
            logger.trace("Probable wrong instance")
        }
    }

    @Topic("HeartbeatRequest")
    public void requestHeartbeat(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing {} for the charger {}", "requestHeartbeat", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
        } else {

            TriggerMessageRequest request = new TriggerMessageRequest()
            request.requestedMessage = TriggerMessageRequestType.Heartbeat

            long sendTimestamp = System.currentTimeMillis(); // Timestamp when the heartbeat is sent
            String uniqueId = sendTimestamp.toString()
            def onSuccess = (CompletionStage<TriggerMessageConfirmation> confirmation) -> {
                long receiveTimestamp = System.currentTimeMillis(); // Timestamp when the response is received
                long latency = receiveTimestamp - sendTimestamp; // Calculate latency

                logger.debug("Latency for charger {}: {} ms", identifier, latency); // Log the latency
                meterRegistry.timer("se.latency", "identifier", identifier).record(Duration.ofMillis(latency));
                // Record latency in metrics

                def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)

                Integer messageTypeId = 3
                ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))

                meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
                publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)
                if (msgConfirmation?.status != TriggerMessageStatus.Accepted) {
                    logger.warn(objectMapper.writeValueAsString(msgConfirmation))
                }
            }

            connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
        }
    }


    /**
     * Sends a Local Authorization List request to a charger.
     * This method takes in a payload containing the Local Authorization List data and sends a request to update the authorization list on a charger, according to the OCPP 1.6 protocol.
     *
     * The payload should be a JSON string with the following structure:
     *{*   "listVersion": <Integer>,  // The version number of the local authorization list.
     *   "updateType": "FULL",   // The type of update to be performed. It should be either "FULL" or "DIFFERENTIAL".
     *   "authList": [
     *{*       "idTag": <String>,    // The identifier of the tag.
     *       "idTagInfo": {        // Information related to the tag.
     *         "status": "Accepted", // The status of the tag. Possible values are defined in AuthorizationStatus enum.
     *         "expiryDate": <String>, // The expiry date of the tag. The string should be in the format compatible with your implementation.
     *         "parentIdTag": <String> // The id of the parent tag. This field is optional.
     *}*},
     *     // More authorization data objects as required...
     *   ]
     *}*
     * @param bucketId The bucket id used as the Kafka key. It should be a unique identifier.
     * @param identifier The identifier of the charger.
     * @param payload The JSON string containing the Local Authorization List data.
     */
    @Topic("SendLocalListRequest")
    public void sendLocalAuthorizationList(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing sendLocalAuthorizationList for the charger {}", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            logger.warn("No session found for identifier: {}", identifier)
        } else {

            try {
                // Parse the payload
                def payloadMap = objectMapper.readValue(payload, Map.class)

                // Validate payload
                if (payloadMap.listVersion == null || payloadMap.updateType == null || payloadMap.authList == null) {
                    logger.warn("Invalid payload received: {}", payload)
                    return
                }

                // Attempt to clear cache before pushing new list
                try {
                    ClearCacheRequest ccrequest = new ClearCacheRequest()
                    ClearCacheConfirmation clearCacheConfirmation = (ClearCacheConfirmation) connections.callInline(UUID.fromString(sessionId), ccrequest)
                } catch (Exception e) {
                    logger.warn("Clear cache request failed for charger {}: {}", identifier, e.getMessage())
                }

                // Create the authorization data
                ArrayList<AuthorizationData> authData = new ArrayList<>()
                payloadMap.authList.each { authDataItem ->
                    AuthorizationData authDataObject = new AuthorizationData(authDataItem.idTag)

                    if (authDataItem.idTagInfo) {
                        IdTagInfo idTagInfoObject = new IdTagInfo()
                        idTagInfoObject.setStatus(AuthorizationStatus.valueOf(authDataItem.idTagInfo.status))
                        if (authDataItem.idTagInfo.expiryDate) {
                            ZonedDateTime expiryDate = ZonedDateTime.parse(authDataItem.idTagInfo.expiryDate)
                            idTagInfoObject.setExpiryDate(expiryDate)
                        }
                        if (authDataItem.idTagInfo.parentIdTag) {
                            idTagInfoObject.setParentIdTag(authDataItem.idTagInfo.parentIdTag)
                        }
                        authDataObject.setIdTagInfo(idTagInfoObject)
                    }

                    authData.add(authDataObject)
                }

                // Check that authData is not empty
                if (authData.isEmpty()) {
                    logger.warn("No authorization data found in payload: {}", payload)
                    return
                }

                // Create the request
                SendLocalListRequest request = new SendLocalListRequest()
                request.setListVersion(payloadMap.listVersion as int)
                request.setUpdateType(UpdateType.valueOf(payloadMap.updateType))
                request.setLocalAuthorizationList(authData.toArray() as AuthorizationData[])

                String uniqueId = System.currentTimeMillis().toString()

                def onSuccess = (CompletionStage<SendLocalListConfirmation> confirmation) -> {
                    def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                    Integer messageTypeId = 3
                    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
                    meterRegistry.counter("se.in", "type", SendLocalListConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": SendLocalListConfirmation.simpleName], 1)

                    if (msgConfirmation.getStatus() != UpdateStatus.Accepted) {
                        logger.warn(objectMapper.writeValueAsString(msgConfirmation))
                    }
                }

                connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
            } catch (JsonProcessingException e) {
                logger.error("Error parsing JSON payload for sendLocalAuthorizationList for charger {}: {}", identifier, payload, e)
            } catch (Exception e) {
                logger.error("Error processing sendLocalAuthorizationList for charger {}: {}", identifier, e, e)
            }
        }
    }


    @Topic("RequestStatus")
    public void requestStatus(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        handleRequestStatusAsync(bucketId, identifier, payload)
    }

    @Async
    CompletableFuture<Void> handleRequestStatusAsync(String bucketId, String identifier, String payload) {
        String uniqueId = System.currentTimeMillis().toString()
        return CompletableFuture.runAsync({
            logger.debug("Processing {} for the charger {}", "requestStatus", identifier)
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            } else {
                logger.debug("[$bucketId]: Requesting status for the charger with session Id ${sessionId}")
                TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.StatusNotification)

                def onSuccess = (CompletionStage<TriggerMessageConfirmation> confirmation) -> {
                    def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                    Integer messageTypeId = 3
                    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
                    meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)

                    if (msgConfirmation.status != TriggerMessageStatus.Accepted) {
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, TriggerMessageRequestType.StatusNotification.toString() + "Error", objectMapper.writeValueAsString(msgConfirmation))
                    }
                }

                connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
            }
        })
    }

    /**
     * Triggers a DiagnosticsStatusNotification request to the charger.
     * Kafka topic: RequestDiagnosticsStatus
     * OCPP: TriggerMessageRequest with requestedMessage = DiagnosticsStatusNotification
     *
     * US-101-02: TriggerMessage Dispatch for DiagnosticsStatusNotification
     * OCPP Compliance: §5.18 TriggerMessageRequest, Appendix 1 (connectorId not applicable)
     *
     * @param bucketId The Kafka partition key (e.g., edgeConfig.kafkaNodeId)
     * @param identifier The charge point identifier
     * @param payload JSON payload (currently unused, reserved for future use)
     */
    @Topic("RequestDiagnosticsStatus")
    public void requestDiagnosticsStatus(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        handleRequestDiagnosticsStatusAsync(bucketId, identifier, payload)
    }

    @Async
    CompletableFuture<Void> handleRequestDiagnosticsStatusAsync(String bucketId, String identifier, String payload) {
        String uniqueId = System.currentTimeMillis().toString()
        return CompletableFuture.runAsync({
            logger.debug("Processing {} for the charger {}", "requestDiagnosticsStatus", identifier)
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                logger.warn("Charger {} not connected, discarding RequestDiagnosticsStatus", identifier)
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            } else {
                logger.debug("[$bucketId]: Requesting diagnostics status for charger with session Id ${sessionId}")
                // OCPP §5.18: TriggerMessageRequest with DiagnosticsStatusNotification
                // Note: connectorId is NOT applicable per OCPP Appendix 1 — omit
                TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.DiagnosticsStatusNotification)

                def onSuccess = (CompletionStage<TriggerMessageConfirmation> confirmation) -> {
                    def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                    Integer messageTypeId = 3
                    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
                    meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)

                    if (msgConfirmation.status != TriggerMessageStatus.Accepted) {
                        logger.warn("TriggerMessage DiagnosticsStatusNotification rejected by charger {}: {}", identifier, msgConfirmation.status)
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, TriggerMessageRequestType.DiagnosticsStatusNotification.toString() + "Error", objectMapper.writeValueAsString(msgConfirmation))
                    }
                    // Note: If Accepted, charger will send DiagnosticsStatusNotification
                    // which is handled by FirmwareEH.handleDiagnosticsStatusNotificationRequest()
                }

                connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
            }
        })
    }

    /**
     * Triggers a FirmwareStatusNotification request to the charger.
     * Kafka topic: RequestFirmwareStatus
     * OCPP: TriggerMessageRequest with requestedMessage = FirmwareStatusNotification
     *
     * US-101-03: TriggerMessage Dispatch for FirmwareStatusNotification
     * OCPP Compliance: §5.18 TriggerMessageRequest, §5.13 FirmwareStatusNotification, Appendix 1 (connectorId not applicable)
     *
     * @param bucketId The Kafka partition key (e.g., edgeConfig.kafkaNodeId)
     * @param identifier The charge point identifier
     * @param payload JSON payload (currently unused, reserved for future use)
     */
    @Topic("RequestFirmwareStatus")
    public void requestFirmwareStatus(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        handleRequestFirmwareStatusAsync(bucketId, identifier, payload)
    }

    @Async
    CompletableFuture<Void> handleRequestFirmwareStatusAsync(String bucketId, String identifier, String payload) {
        String uniqueId = System.currentTimeMillis().toString()
        return CompletableFuture.runAsync({
            logger.debug("Processing {} for the charger {}", "requestFirmwareStatus", identifier)
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                logger.warn("Charger {} not connected, discarding RequestFirmwareStatus", identifier)
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            } else {
                logger.debug("[$bucketId]: Requesting firmware status for charger with session Id ${sessionId}")
                // OCPP §5.18: TriggerMessageRequest with FirmwareStatusNotification
                // Note: connectorId is NOT applicable per OCPP Appendix 1 — omit
                TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.FirmwareStatusNotification)

                def onSuccess = (CompletionStage<TriggerMessageConfirmation> confirmation) -> {
                    def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                    Integer messageTypeId = 3
                    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
                    meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)

                    if (msgConfirmation.status != TriggerMessageStatus.Accepted) {
                        logger.warn("TriggerMessage FirmwareStatusNotification rejected by charger {}: {}", identifier, msgConfirmation.status)
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, TriggerMessageRequestType.FirmwareStatusNotification.toString() + "Error", objectMapper.writeValueAsString(msgConfirmation))
                    }
                    // Note: If Accepted, charger will send FirmwareStatusNotification
                    // which is handled by FirmwareEH.handleFirmwareStatusNotificationRequest()
                }

                connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
            }
        })
    }

    @Topic("RequestMeterValues")
    public void requestMeterValues(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing {} for the charger {}", "requestMeterValues", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        logger.debug("Requesting meter values for the charger with session Id ${sessionId} and connector $payload")
        def params = objectMapper.readValue(payload, Map.class)
        TriggerMessageRequest request = new TriggerMessageRequest(requestedMessage: TriggerMessageRequestType.MeterValues, connectorId: params['connectorId'])
        String uniqueId = System.currentTimeMillis().toString()
        def onSuccess = (CompletionStage<TriggerMessageConfirmation> confirmation) -> {
            def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
            Integer messageTypeId = 3
            ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
            meterRegistry.counter("se.in", "type", TriggerMessageConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": TriggerMessageConfirmation.simpleName], 1)

            if (msgConfirmation.status != TriggerMessageStatus.Accepted) {
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, TriggerMessageRequestType.MeterValues.toString() + "Error", objectMapper.writeValueAsString(msgConfirmation))
            }
        }

        connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
    }

    @Topic("RequestConfiguration")
    public void requestConfiguration(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing {} for the charger {}", "requestConfiguration", identifier)
        try {
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }
            Map requestPayload = objectMapper.readValue(payload, Map.class)
            String[] keys = requestPayload.key

            logger.trace("Attempting to fetch configuration for charger with sessionId ${sessionId}")
            GetConfigurationRequest request = new GetConfigurationRequest()
            if(keys){
                logger.info("RequestConfiguration -> Keys length $keys.length, and keys are $keys")
                request.setKey(keys)
            }

            List configuration = ((GetConfigurationConfirmation) connections.callInline(UUID.fromString(sessionId), request))?.configurationKey?.collect({
                [key: it.key, value: it.value, readonly: it.readonly]
            })
            meterRegistry.counter("se.in", "type", GetConfigurationConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": GetConfigurationConfirmation.simpleName], 1)
            incoming.chargerConfiguration(edgeConfig.kafkaNodeId, identifier, "ConfigurationReceived", objectMapper.writeValueAsString(configuration))
        } catch (NotConnectedException e) {
            logger.error("Configuration cannot be retrieved for an offline charger ", e)
        } catch (Exception e) {
            logger.error("Unknown error while fetching configuration ", e)
        }
    }

    @Topic("UpdateConfiguration")
    public void updateConfiguration(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing {} for the charger {}", "updateConfiguration", identifier)
        try {
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }

            Map requestPayload = objectMapper.readValue(payload, Map.class)
            ChangeConfigurationRequest request = new ChangeConfigurationRequest(key: requestPayload.key, value: requestPayload.value ?: "")
            ChangeConfigurationConfirmation configurationConfirmation = (ChangeConfigurationConfirmation) connections.callInline(UUID.fromString(sessionId), request)
            meterRegistry.counter("se.in", "type", ChangeConfigurationConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": ChangeConfigurationConfirmation.simpleName], 1)
            Map responsePayload=[payload:requestPayload,statusPayload:configurationConfirmation]
            logger.debug("configurationUpdate responsePayload==>${objectMapper.writeValueAsString(responsePayload)}")
            incoming.configurationUpdate(edgeConfig.kafkaNodeId, identifier,objectMapper.writeValueAsString(responsePayload))

            // US-CERT106-03: SecurityProfile upgrade handling (TC-64)
            if (requestPayload.key == "SecurityProfile") {
                int targetProfile = Integer.parseInt(requestPayload.value ?: "0")
                if (configurationConfirmation.status == ConfigurationStatus.Accepted) {
                    connections.markSecurityProfileUpgrade(identifier, targetProfile)
                    logger.info("SecurityProfile upgrade to Profile {} accepted by {}", targetProfile, identifier)
                }
                if (targetProfile == 3 && !edgeConfig.mtls.enabled) {
                    logger.warn("SecurityProfile upgrade to Profile 3 requested for {} but mTLS is not configured (se.config.mtls.enabled=false) — command dispatched per OCPP spec", identifier)
                }
            }

            if (configurationConfirmation.status == ConfigurationStatus.RebootRequired) {
                this.requestBoot("000", sessionId, "TriggerBootRequest", "{}")
                logger.trace("Rebooting the charger after configuration update")
            } else if (configurationConfirmation.status == ConfigurationStatus.Rejected || configurationConfirmation.status == ConfigurationStatus.NotSupported) {
                logger.warn("Configuration cannot be updated ", configurationConfirmation.status.toString())
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "ConfigurationUpdateError", objectMapper.writeValueAsString(configurationConfirmation))
            } else {
                logger.trace("Successfully updated the configuration")
            }
        } catch (NotConnectedException e) {
            logger.error("Configuration cannot be updated for an offline charger ", e)
        } catch (Exception e) {
            logger.error("Unknown error while updating configuration ", e)
        }
    }

    @Topic("ClearCache")
    public void clearCache(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload) {
        logger.debug("Processing {} for the charger {}", "clearCache", identifier)
        try {
            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }

            ClearCacheRequest request = new ClearCacheRequest()
            ClearCacheConfirmation clearCacheConfirmation = (ClearCacheConfirmation) connections.callInline(UUID.fromString(sessionId), request)
            meterRegistry.counter("se.in", "type", ClearCacheConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": ClearCacheConfirmation.simpleName], 1)

            // Check the appropriate statuses based on ClearCacheConfirmation response
            if (clearCacheConfirmation.status == ClearCacheStatus.Accepted) {
                logger.trace("Successfully cleared the cache")
            } else {
                logger.warn("Cache cannot be cleared ", clearCacheConfirmation.status.toString())
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "ClearCacheError", objectMapper.writeValueAsString(clearCacheConfirmation))
            }
        } catch (NotConnectedException e) {
            logger.error("Cache cannot be cleared for an offline charger ", e)
        } catch (Exception e) {
            logger.error("Unknown error while clearing cache ", e)
        }
    }

    @Topic("RemoteTransaction")
    public void remoteTransaction(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                  @MessageHeader("eventType") String eventType, @MessageHeader("qrCode") String qrCode, String payload) {
        logger.debug("Processing {} for the charger {} of type {}", "remoteTransaction", identifier, eventType)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        logger.debug("Triggering ${eventType} for session ${sessionId}")
        Map requestPayload = objectMapper.readValue(payload, Map.class)
        switch (eventType) {
            case OCPPEventTypes.REMOTE_START_TRANSACTION.toString():
                // US-RCP102-01: Validate and attach chargingProfile if present
                ChargingProfile chargingProfile = null
                if (requestPayload.containsKey("chargingProfile") && requestPayload.get("chargingProfile")) {
                    try {
                        // Directly convert the map to OCPP ChargingProfile
                        chargingProfile = objectMapper.convertValue(requestPayload.get("chargingProfile"), ChargingProfile.class)

                        Integer connectorId = requestPayload.get("connectorIndex") as Integer
                        String validationError = validateTxProfile(chargingProfile, connectorId)
                        if (validationError) {
                            logger.warn("TxProfile validation failed for charger {}: {}", identifier, validationError)
                            incoming.chargerError(edgeConfig.kafkaNodeId, identifier,
                                OCPPEventTypes.REMOTE_START_TRANSACTION.toString() + "Error",
                                objectMapper.writeValueAsString([error: validationError, connectorId: connectorId]))
                            return
                        }

                        // Strip transactionId — not known at remote start time (OCPP §3.13)
                        chargingProfile.transactionId = null
                    } catch (Exception e) {
                        logger.error("Failed to convert chargingProfile for charger {}: {}, payload: {}",
                            identifier, e.message, objectMapper.writeValueAsString(requestPayload.get("chargingProfile")), e)
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier,
                            OCPPEventTypes.REMOTE_START_TRANSACTION.toString() + "Error",
                            objectMapper.writeValueAsString([error: "Invalid chargingProfile format: ${e.message}"]))
                        return
                    }
                }

                RemoteStartTransactionRequest remoteStartRequest = new RemoteStartTransactionRequest(connectorId: requestPayload.get("connectorIndex"),
                        idTag: requestPayload.get("idTag"))

                // US-RCP102-01: Set chargingProfile on request if present and validated
                if (chargingProfile) {
                    remoteStartRequest.chargingProfile = chargingProfile
                }

                def remoteStartConfirmation
                try {
                    remoteStartConfirmation = (RemoteStartTransactionConfirmation) connections.callInline(UUID.fromString(sessionId), remoteStartRequest)
                    meterRegistry.counter("se.in", "type", RemoteStartTransactionConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": RemoteStartTransactionConfirmation.simpleName], 1)
                    ProtocolLog.debug(true, connections.getChargerId(sessionId), "RemoteStartStopStatus -${RemoteStartStopStatus.Accepted}")
                    if (remoteStartConfirmation?.status != RemoteStartStopStatus.Accepted) {
                        ProtocolLog.debug(true,connections.getChargerId(sessionId),"RemoteStartConfirmation failed - ${remoteStartConfirmation?.status}")
                        def errorPayload = remoteStartConfirmation?.properties ?: [:]
                        errorPayload["connectorId"] = requestPayload.get("connectorIndex")
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString() + "Error", objectMapper.writeValueAsString(remoteStartConfirmation ?: [:]))
                        incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, "${OCPPEventTypes.REMOTE_START_TRANSACTION.toString()}Failed", "UNKNOWN", objectMapper.writeValueAsString(errorPayload))
                    }
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_START_TRANSACTION_RESPONSE.toString(),
                            qrCode, objectMapper.writeValueAsString(["status": remoteStartConfirmation?.status?.toString(), chargeSessionId: requestPayload.chargeSessionId]))

                } catch (NotConnectedException nce) {
                    //NCE is reported at trace level
                    logger.trace("Probable wrong instance")
                } catch (Exception e) {
                    logger.error("Error starting transaction " + e)
                    def errorPayload = remoteStartConfirmation?.properties ?: [:]
                    errorPayload["connectorId"] = requestPayload.get("connectorIndex")
                    incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString() + "Error", objectMapper.writeValueAsString(remoteStartConfirmation ?: [:]))
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, "${OCPPEventTypes.REMOTE_START_TRANSACTION.toString()}Failed", "UNKNOWN", objectMapper.writeValueAsString(errorPayload))
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_START_TRANSACTION_RESPONSE.toString(),
                            qrCode, objectMapper.writeValueAsString(["status": "Error", chargeSessionId: requestPayload.chargeSessionId]))
                }
                break
            case OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString():
                RemoteStopTransactionRequest remoteStopRequest = new RemoteStopTransactionRequest(transactionId: requestPayload.get("seTransactionId"))

                def remoteStopConfirmation = null
                try {
                    remoteStopConfirmation = (RemoteStopTransactionConfirmation) connections.callInline(UUID.fromString(sessionId), remoteStopRequest)
                    meterRegistry.counter("se.in", "type", RemoteStopTransactionConfirmation.simpleName).increment()
                    publishMetricsCounterIncrement("se.in", ["type": RemoteStopTransactionConfirmation.simpleName], 1)
                    if (remoteStopConfirmation?.status != RemoteStartStopStatus.Accepted) {
                        def errorPayload = remoteStopConfirmation?.properties ?: [:]
                        errorPayload["seTransactionId"] = requestPayload.get("seTransactionId")
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString() + "Error", objectMapper.writeValueAsString(remoteStopConfirmation ?: [:]))
                        incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, "${OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString()}Failed", "UNKNOWN", objectMapper.writeValueAsString(errorPayload))
                    }
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_STOP_TRANSACTION_RESPONSE.toString(),
                            qrCode, objectMapper.writeValueAsString(["status": remoteStopConfirmation?.status?.toString(), chargeSessionId: requestPayload.chargeSessionId, seTransactionId: requestPayload.get("seTransactionId")]))
                } catch (NotConnectedException nce) {
                    //NCE is reported at trace level
                    logger.trace("Probable wrong instance")
                } catch (Exception e) {
                    logger.error("Error stopping transaction ", e)
                    def errorPayload = remoteStopConfirmation?.properties ?: [:]
                    errorPayload["seTransactionId"] = requestPayload.get("seTransactionId")
                    incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString() + "Error", objectMapper.writeValueAsString(remoteStopConfirmation ?: [:]))
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, "${OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString()}Failed", "UNKNOWN", objectMapper.writeValueAsString(errorPayload))
                    incoming.ocppTransaction(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.REMOTE_STOP_TRANSACTION_RESPONSE.toString(),
                            qrCode, objectMapper.writeValueAsString(["status": "Error", chargeSessionId: requestPayload.chargeSessionId, seTransactionId: requestPayload.get("seTransactionId")]))
                }
                break
        }
    }

    /**
     *
     * @param bucketId
     * @param sessionId
     * @param payload{resetType: "Soft/Hard"}
     */
    @Topic("ResetRequest")
    public void requestReset(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                             String payload) {
        logger.debug("Processing {} for the charger {}", "resetRequest", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        Map requestPayload = objectMapper.readValue(payload, Map.class)
        ResetRequest resetRequest = new ResetRequest();
        resetRequest.setType(ResetType.valueOf(requestPayload.get("resetType")))
        String uniqueId = System.currentTimeMillis().toString()
        def onSuccess = (CompletionStage<ResetConfirmation> confirmation) -> {
            def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
            Integer messageTypeId = 3
            ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
            meterRegistry.counter("se.in", "type", ResetConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": ResetConfirmation.simpleName], 1)

            if (msgConfirmation.status == ResetStatus.Rejected) {
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "Reset${resetRequest.type?.toString()}Error", objectMapper.writeValueAsString(msgConfirmation))
            }
        }

        connections.pushCommandWithDelay(UUID.fromString(sessionId), resetRequest, onSuccess, 0, uniqueId)
    }

    /**
     *
     * @param bucketId
     * @param sessionId
     * @param payload - Empty JSON {}
     */
    @Topic("ReconnectRequest")
    public void requestReconnect(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                 String payload) {
        logger.debug("Processing {} for the charger {}", "requestReconnect", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        connections.closeSession(UUID.fromString(sessionId))
    }

    /**
     *
     * @param bucketId
     * @param sessionId
     * @param payload - Empty JSON {}
     */
    @Topic("DownloadDiagnosticLogs")
    public void requestDiagnosticLogs(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                      String payload) {
        logger.debug("Processing {} for the charger {}", "requestDiagnosticLogs", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        logger.debug("Requesting diagnostic logs for the charger with session Id ${sessionId} and will be downloaded to ${edgeConfig.chargers.diagnosticLogLocation}")
        if (edgeConfig.chargers.diagnosticLogLocation && edgeConfig.chargers.diagnosticLogLocation != "") {
            ProtocolLog.debug(false, identifier, "Requesting diagnostic logs")
            GetDiagnosticsRequest request = new GetDiagnosticsRequest()
            request.setLocation(edgeConfig.chargers.diagnosticLogLocation)
            String uniqueId = System.currentTimeMillis().toString()
            def onSuccess = (CompletionStage<GetDiagnosticsConfirmation> confirmation) -> {
                def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                Integer messageTypeId = 3
                ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
                meterRegistry.counter("se.in", "type", GetDiagnosticsConfirmation.simpleName).increment()
                publishMetricsCounterIncrement("se.in", ["type": GetDiagnosticsConfirmation.simpleName], 1)

                if (confirmation) incoming.diagnosticsConfirmed(edgeConfig.kafkaNodeId, identifier, objectMapper.writeValueAsString([fileName: msgConfirmation.fileName]))

                if (!confirmation || !msgConfirmation?.fileName) {
                    incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "RequestDiagnosticLogsError", confirmation ? objectMapper.writeValueAsString(confirmation) : "{}")
                }
            }

            connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
        } else {
            logger.error("Improperly configured diagnostic log download location", edgeConfig.chargers.diagnosticLogLocation)
        }
    }

    @Topic("UnlockConnector")
    public void unlockConnector(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                String payload) {
        logger.debug("Processing {} for the charger {}", "unlockConnector", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        Map requestPayload = objectMapper.readValue(payload, Map.class)

        // OCPP 1.6 §6.21: connectorId MUST be an integer > 0. ConnectorId 0 refers to the whole
        // charge point and is explicitly disallowed for UnlockConnector.
        Integer connectorId = requestPayload['connectorIndex'] as Integer
        if (connectorId == null || connectorId <= 0) {
            logger.error("UnlockConnector rejected for charger {} — invalid connectorId: {}. " +
                    "OCPP 1.6 §6.21 requires connectorId > 0.", identifier, connectorId)
            meterRegistry.counter("se.unlock.invalid.connectorid", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.unlock.invalid.connectorid", ["identifier": identifier], 1)
            return
        }

        UnlockConnectorRequest request = new UnlockConnectorRequest(connectorId: connectorId)
        String uniqueId = System.currentTimeMillis().toString()

        // OCPP 1.6 §6.21: CS sends UnlockConnector.req; CP replies with UnlockConnector.conf
        // containing status: Unlocked | UnlockFailed | NotSupported
        def onSuccess = (CompletionStage<UnlockConnectorConfirmation> confirmation) -> {
            def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
            Integer messageTypeId = 3
            ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
            meterRegistry.counter("se.in", "type", UnlockConnectorConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": UnlockConnectorConfirmation.simpleName], 1)

            String responseJson = objectMapper.writeValueAsString(msgConfirmation)

            if (msgConfirmation.status == UnlockStatus.Unlocked) {
                // Notify downstream CSMS services of successful unlock
                logger.info("UnlockConnector succeeded for charger {} connectorId {}", identifier, connectorId)
                incoming.unlockConnectorResponse(edgeConfig.kafkaNodeId, identifier, "UnlockConnectorResponse", responseJson)
            } else {
                // OCPP 1.6 §6.21: UnlockFailed or NotSupported — surface the error downstream
                logger.warn("UnlockConnector failed for charger {} connectorId {} — status: {}",
                        identifier, connectorId, msgConfirmation.status)
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "UnlockConnectorError", responseJson)
            }
        }

        connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
    }

    /**
     *
     * @param bucketId
     * @param sessionId
     * @param payload - {location: 'Firmware ftp Url including filename', when: 'date in yyyy-MM-dd'T'HH:mm:ssZ format'
     *  and can be customized with an externalized property}
     */
    @Topic("UpdateFirmware")
    public void updateFirmware(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                               String payload) {
        logger.debug("Processing {} for the charger {}", "updateFirmware", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        def params = objectMapper.readValue(payload, Map.class)
        def location = params?.get("location")

        if (location) {
            def rightNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(),
                    ZoneId.systemDefault())
            def retrieveDate = params.get("when") ? this.parse(params.get("when")) : null

            UpdateFirmwareRequest request = new UpdateFirmwareRequest(location: location, retrieveDate: retrieveDate ?: rightNow,retries: 3, retryInterval: 30)
            String uniqueId = System.currentTimeMillis().toString()
            def onSuccess = (CompletionStage<UpdateFirmwareConfirmation> confirmation) -> {
                def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
                meterRegistry.counter("se.in", "type", UpdateFirmwareConfirmation.simpleName).increment()
                publishMetricsCounterIncrement("se.in", ["type": UpdateFirmwareConfirmation.simpleName], 1)

                Integer messageTypeId = 3
                ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
            }
            connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
        }
    }

    @Topic("CloseSocket")
    public void closeSocket(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                            String payload) {
        def sessionId = connections.getLatestSessionId(identifier)
        logger.debug("Processing {} for the charger {}", "closeSocket", identifier)
        try {
            if (!sessionId) {
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }
            logger.debug("Closing socket session for the identifier ${identifier}")
            connections.closeSession(UUID.fromString(sessionId))
        } catch (NotConnectedException nce) {
            logger.trace("Probable wrong instance")
        }
    }

    @Topic("SEScheduler")
    public void scheduler(@KafkaKey String bucketId, @MessageHeader("type") String type, String payload) {
        logger.debug("Processing scheduler {}", type)
        if ("refreshStatuses".equalsIgnoreCase(type)) {
            connections.refreshStatuses()
        }
        if ("fetchConfig".equalsIgnoreCase(type)) {
            authCacheService.fetchChargerAuthConfig()

        }

    }

    @Topic("ChargingProfile")
    public void chargingProfile(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, @MessageHeader("eventType") String eventType, String payload) {
        logger.debug("Processing {} for the charger {}", eventType, identifier)
        switch (eventType) {
            case OCPPEventTypes.SET_CHARGING_PROFILE.toString():
                chargingProfileService.setChargingProfile(identifier, payload)
                break
            case OCPPEventTypes.CLEAR_CHARGING_PROFILE.toString():
                chargingProfileService.clearChargingProfile(identifier, payload)
                break
            case OCPPEventTypes.GET_CHARGING_PROFILE.toString():
                chargingProfileService.getCompositeSchedule(identifier, payload)
                break
        }
    }

    /**
     * Consumes certificate management commands from admin-service and forwards them to
     * the connected charge point via OCPP 1.6 SecurityExt profile.
     *
     * Supported eventTypes: InstallCertificate, DeleteCertificate, GetInstalledCertificateIds
     *
     * US-CERT105-01: SecurityExt Certificate Management Command Handlers
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1, §4.2, §4.3
     * [OCA-CERT-RELEVANT] TC-57, TC-58, TC-59
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param eventType One of: InstallCertificate, DeleteCertificate, GetInstalledCertificateIds
     * @param payload JSON command payload
     */
    @Topic("CertificateManagement")
    void certificateManagement(@KafkaKey String bucketId,
                               @MessageHeader("identifier") String identifier,
                               @MessageHeader("eventType") String eventType,
                               String payload) {
        logger.debug("Processing certificateManagement for charger {} eventType {}", identifier, eventType)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            def errorResponse = [chargePointId: identifier, status: "ChargePointNotConnected", operationType: eventType]
            incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
                "${eventType}Response", objectMapper.writeValueAsString(errorResponse))
            logger.warn("No session for {} — CertificateManagement {} not sent", identifier, eventType)
            return
        }

        try {
            switch (eventType) {
                case "InstallCertificate":
                    handleInstallCertificate(sessionId, identifier, payload)
                    break
                case "DeleteCertificate":
                    handleDeleteCertificate(sessionId, identifier, payload)
                    break
                case "GetInstalledCertificateIds":
                    handleGetInstalledCertificateIds(sessionId, identifier, payload)
                    break
                case "ExtendedTriggerMessage":
                    handleExtendedTriggerMessage(sessionId, identifier, payload)
                    break
                case "CertificateSigned":
                    handleCertificateSigned(sessionId, identifier, payload)
                    break
                default:
                    logger.error("Unknown CertificateManagement eventType: {}", eventType)
            }
        } catch (NotConnectedException nce) {
            logger.trace("Probable wrong instance for {} — {}", identifier, eventType)
        } catch (Exception e) {
            logger.error("CertificateManagement error for {} — {}: {} | Payload: {}", identifier, eventType, e.message, payload)
            def errorResponse = [chargePointId: identifier, status: "Error", operationType: eventType, error: e.message]
            incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
                "${eventType}Response", objectMapper.writeValueAsString(errorResponse))
        }
    }

    /**
     * Consumes GetLog commands from admin-service and forwards them to
     * the connected charge point via OCPP 1.6 SecurityExt profile.
     *
     * [OCPP-COMPLIANCE] Security Whitepaper §4.1 — UC04 GetLog
     * [OCA-CERT-RELEVANT] TC-61
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param payload JSON command payload
     */
    @Topic("GetLog")
    void getLog(@KafkaKey String bucketId,
                @MessageHeader("identifier") String identifier,
                String payload) {
        logger.debug("Processing GetLog for charger {}", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            def errorResponse = [chargePointId: identifier, status: "ChargePointNotConnected", operationType: "GetLog"]
            incoming.getLogResponse(edgeConfig.kafkaNodeId, identifier,
                "GetLogResponse", objectMapper.writeValueAsString(errorResponse))
            logger.warn("No session for {} — GetLog not sent", identifier)
            return
        }

        try {
            handleGetLog(sessionId, identifier, payload)
        } catch (NotConnectedException nce) {
            logger.trace("Probable wrong instance for {} — GetLog", identifier)
        } catch (Exception e) {
            logger.error("GetLog error for {}: {} | Payload: {}", identifier, e.message, payload)
            def errorResponse = [chargePointId: identifier, status: "Error", operationType: "GetLog", error: e.message]
            incoming.getLogResponse(edgeConfig.kafkaNodeId, identifier,
                "GetLogResponse", objectMapper.writeValueAsString(errorResponse))
        }
    }

    /**
     * Consumes SignedUpdateFirmware commands from admin-service and forwards them to
     * the connected charge point via OCPP 1.6 SecurityExt profile.
     *
     * [OCPP-COMPLIANCE] Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
     * [OCA-CERT-RELEVANT] TC-62, TC-63
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param payload JSON command payload
     */
    @Topic("SignedUpdateFirmware")
    void signedUpdateFirmware(@KafkaKey String bucketId,
                              @MessageHeader("identifier") String identifier,
                              String payload) {
        logger.debug("Processing SignedUpdateFirmware for charger {}", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            def errorResponse = [chargePointId: identifier, status: "ChargePointNotConnected", operationType: "SignedUpdateFirmware"]
            incoming.signedUpdateFirmwareResponse(edgeConfig.kafkaNodeId, identifier,
                "SignedUpdateFirmwareResponse", objectMapper.writeValueAsString(errorResponse))
            logger.warn("No session for {} — SignedUpdateFirmware not sent", identifier)
            return
        }

        try {
            handleSignedUpdateFirmware(sessionId, identifier, payload)
        } catch (NotConnectedException nce) {
            logger.trace("Probable wrong instance for {} — SignedUpdateFirmware", identifier)
        } catch (Exception e) {
            logger.error("SignedUpdateFirmware error for {}: {} | Payload: {}", identifier, e.message, payload)
            def errorResponse = [chargePointId: identifier, status: "Error", operationType: "SignedUpdateFirmware", error: e.message]
            incoming.signedUpdateFirmwareResponse(edgeConfig.kafkaNodeId, identifier,
                "SignedUpdateFirmwareResponse", objectMapper.writeValueAsString(errorResponse))
        }
    }

    /**
     * Handles InstallCertificate command — installs a root CA certificate on the charge point.
     * [OCPP-COMPLIANCE] Security Whitepaper §4.1
     */
    private void handleInstallCertificate(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)
        def certType = CertificateUseEnumType.valueOf(payloadMap.certificateType as String)
        def request = new InstallCertificateRequest(certType, payloadMap.certificate as String)

        def confirmation = (InstallCertificateConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId: identifier,
            certificateType: payloadMap.certificateType,
            status: confirmation?.status?.toString() ?: "Error",
            correlationId: payloadMap?.correlationId
        ]
        incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
            "InstallCertificateResponse", objectMapper.writeValueAsString(response))
        logger.info("InstallCertificate for {} — status: {}", identifier, confirmation?.status)
    }

    /**
     * Handles DeleteCertificate command — deletes a certificate from the charge point by hash.
     * [OCPP-COMPLIANCE] Security Whitepaper §4.2
     */
    private void handleDeleteCertificate(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)
        def hashData = new CertificateHashDataType()
        hashData.hashAlgorithm = HashAlgorithmEnumType.valueOf(payloadMap.hashAlgorithm as String)
        hashData.issuerNameHash = payloadMap.issuerNameHash as String
        hashData.issuerKeyHash = payloadMap.issuerKeyHash as String
        hashData.serialNumber = payloadMap.serialNumber as String
        def request = new DeleteCertificateRequest(hashData)

        def confirmation = (DeleteCertificateConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId: identifier,
            status: confirmation?.status?.toString() ?: "Error"
        ]
        incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
            "DeleteCertificateResponse", objectMapper.writeValueAsString(response))
        logger.info("DeleteCertificate for {} — status: {}", identifier, confirmation?.status)
    }

    /**
     * Handles GetInstalledCertificateIds command — queries installed certificates from the charge point.
     * [OCPP-COMPLIANCE] Security Whitepaper §4.3
     */
    private void handleGetInstalledCertificateIds(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)
        def certType = CertificateUseEnumType.valueOf(payloadMap.certificateType as String)
        def request = new GetInstalledCertificateIdsRequest(certType)

        def confirmation = (GetInstalledCertificateIdsConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def certHashList = confirmation?.certificateHashData?.collect { hashData ->
            [
                hashAlgorithm: hashData.hashAlgorithm?.toString(),
                issuerNameHash: hashData.issuerNameHash,
                issuerKeyHash: hashData.issuerKeyHash,
                serialNumber: hashData.serialNumber
            ]
        } ?: []

        def response = [
            chargePointId: identifier,
            status: confirmation?.status?.toString() ?: "Error",
            certificateType: certType,
            certificateHashData: certHashList
        ]
        incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
            "GetInstalledCertificateIdsResponse", objectMapper.writeValueAsString(response))
        logger.info("GetInstalledCertificateIds for {} — status: {}, count: {}", identifier, confirmation?.status, certHashList.size())
    }

    /**
     * Handles ExtendedTriggerMessage command — triggers the charge point to send a specific message
     * (e.g. SignCertificate for certificate renewal).
     *
     * US-CERT106-02: ExtendedTriggerMessage handler for TC-68 certificate renewal flow.
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3 — Step 1: Trigger CSR generation
     */
    private void handleExtendedTriggerMessage(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)
        def requestedMessage = payloadMap.requestedMessage as String
        def request = new ExtendedTriggerMessageRequest(MessageTriggerEnumType.valueOf(requestedMessage))

        def confirmation = (ExtendedTriggerMessageConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId   : identifier,
            requestedMessage: requestedMessage,
            status          : confirmation?.status?.toString() ?: "Error"
        ]
        incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
            "ExtendedTriggerMessageResponse", objectMapper.writeValueAsString(response))
        logger.info("ExtendedTriggerMessage({}) for {} — status: {}", requestedMessage, identifier, confirmation?.status)
    }

    /**
     * Handles CertificateSigned command — delivers the signed certificate chain to the charge point.
     *
     * US-CERT106-02: CertificateSigned handler for TC-68 certificate renewal flow.
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3 — Step 3: Deliver signed certificate
     */
    private void handleCertificateSigned(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)
        def certificateChain = payloadMap.certificateChain as String
        def request = new CertificateSignedRequest(certificateChain)

        def confirmation = (CertificateSignedConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId: identifier,
            status       : confirmation?.status?.toString() ?: "Error"
        ]
        incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, identifier,
            "CertificateSignedResponse", objectMapper.writeValueAsString(response))

        if (confirmation?.status?.toString() == "Rejected") {
            logger.warn("CertificateSigned for {} — Rejected (possible key mismatch or invalid chain)", identifier)
        } else {
            logger.info("CertificateSigned for {} — status: {}", identifier, confirmation?.status)
        }
    }

    /**
     * Handles GetLog command — requests log upload from charge point.
     * [OCPP-COMPLIANCE] Security Whitepaper §4.1 — UC04 GetLog
     * [OCA-CERT-RELEVANT] TC-61
     */
    private void handleGetLog(String sessionId, String identifier, String payload) {
        def payloadMap = objectMapper.readValue(payload, Map)

        def logParams = new LogParametersType()
        logParams.remoteLocation = payloadMap.remoteLocation as String
        if (payloadMap.oldestTimestamp) {
            logParams.oldestTimestamp = java.time.ZonedDateTime.parse(payloadMap.oldestTimestamp as String)
        }
        if (payloadMap.latestTimestamp) {
            logParams.latestTimestamp = java.time.ZonedDateTime.parse(payloadMap.latestTimestamp as String)
        }

        def logType = LogEnumType.valueOf(payloadMap.logType as String)
        def requestId = payloadMap.requestId as Integer

        def request = new GetLogRequest(logType, requestId, logParams)
        if (payloadMap.retries) {
            request.retries = payloadMap.retries as Integer
        }
        if (payloadMap.retryInterval) {
            request.retryInterval = payloadMap.retryInterval as Integer
        }

        def confirmation = (GetLogConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId: identifier,
            logType      : payloadMap.logType,
            requestId    : requestId,
            status       : confirmation?.status?.toString() ?: "Error"
        ]
        incoming.getLogResponse(edgeConfig.kafkaNodeId, identifier,
            "GetLogResponse", objectMapper.writeValueAsString(response))

        if ("Rejected" == confirmation?.status?.toString()) {
            logger.warn("GetLog rejected for {} — logType: {}, requestId: {}, filename: {}", identifier, payloadMap.logType, requestId, confirmation.filename)
        } else {
            logger.info("GetLog for {} — status: {}, logType: {}, requestId: {}, filename: {}", identifier, confirmation?.status, payloadMap.logType, requestId, confirmation.filename)
        }
    }

    /**
     * Handles SignedUpdateFirmware command — sends signed firmware update to charge point.
     * [OCPP-COMPLIANCE] Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
     * [OCA-CERT-RELEVANT] TC-62, TC-63
     */
    private void handleSignedUpdateFirmware(String sessionId, String identifier, String payload) {
        logger.debug("SignedUpdateFirmware payload for {}: {}", identifier, payload)
        def payloadMap = objectMapper.readValue(payload, Map)
        def firmwareMap = payloadMap.firmware as Map

        def firmware = new FirmwareType()
        firmware.location = firmwareMap.location as String
        firmware.retrieveDateTime = firmwareMap.retrieveDateTime?java.time.ZonedDateTime.parse(firmwareMap.retrieveDateTime as String):ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(),
                ZoneId.systemDefault())
        if (firmwareMap.installDateTime) {
            firmware.installDateTime = java.time.ZonedDateTime.parse(firmwareMap.installDateTime as String)
        }else{
            firmwareMap.installDateTime = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(),
                    ZoneId.systemDefault())
        }
        firmware.signingCertificate = firmwareMap.signingCertificate as String
        firmware.signature = firmwareMap.signature as String

        def requestId = payloadMap.requestId as Integer
        def request = new SignedUpdateFirmwareRequest(requestId, firmware)
        if (payloadMap.retries) {
            request.retries = payloadMap.retries as Integer
        }
        if (payloadMap.retryInterval) {
            request.retryInterval = payloadMap.retryInterval as Integer
        }

        def confirmation = (SignedUpdateFirmwareConfirmation) connections.callInline(
            UUID.fromString(sessionId), request)

        def response = [
            chargePointId: identifier,
            requestId    : requestId,
            status       : confirmation?.status?.toString() ?: "Error"
        ]
        incoming.signedUpdateFirmwareResponse(edgeConfig.kafkaNodeId, identifier,
            "SignedUpdateFirmwareResponse", objectMapper.writeValueAsString(response))

        def confStatus = confirmation?.status?.toString()
        if (confStatus in ["Rejected", "InvalidCertificate", "RevokedCertificate"]) {
            logger.warn("SignedUpdateFirmware for {} — status: {}, requestId: {}", identifier, confStatus, requestId)
        } else {
            logger.info("SignedUpdateFirmware for {} — status: {}, requestId: {}", identifier, confStatus, requestId)
        }
    }

    /**
     * OCPP 1.6 §3.11 ReserveNow.
     * Receives a ReserveNow command from charge-session-monitor and delegates
     * to ReservationService to send the OCPP request to the charger.
     */
    @Topic("ReserveNow")
    public void reserveNow(@KafkaKey String bucketId,
                           @MessageHeader("identifier") String identifier,
                           @MessageHeader("eventType") String eventType,
                           String payload) {
        logger.debug("Processing {} for charger {} eventType {}", "reserveNow", identifier, eventType)
        reservationService.reserveNow(identifier, payload)
    }

    /**
     * OCPP 1.6 §3.3 CancelReservation.
     * Receives a CancelReservation command from charge-session-monitor and delegates
     * to ReservationService to send the OCPP request to the charger.
     */
    @Topic("CancelReservation")
    public void cancelReservation(@KafkaKey String bucketId,
                                  @MessageHeader("identifier") String identifier,
                                  @MessageHeader("eventType") String eventType,
                                  String payload) {
        logger.debug("Processing {} for charger {} eventType {}", "cancelReservation", identifier, eventType)
        reservationService.cancelReservation(identifier, payload)
    }

    @Topic("DataTransferRequest")
    void dataTransferRequest(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                             String payload) {
        logger.debug("Processing {} for the charger {}", "DataTransferRequest", identifier)
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        logger.debug("[$bucketId]: DataTransfer request for the charger with session Id ${sessionId}")
        Map payloadMap = objectMapper.readValue(payload, Map.class)

        def vendorId = payloadMap["vendorId"] ? payloadMap["vendorId"] : "org.openchargealliance.costmsg"
        def messageId = payloadMap["purpose"] ?:""
        DataTransferRequest request = new DataTransferRequest(vendorId: vendorId, "messageId": messageId, data: payloadMap["details"])
        String uniqueId = System.currentTimeMillis().toString()
        def onSuccess = (CompletionStage<DataTransferConfirmation> confirmation) -> {
            def msgConfirmation = confirmation.toCompletableFuture().get(SocketSessionEvents.WS_MSG_WAIT_PERIOD, TimeUnit.SECONDS)
            Integer messageTypeId = 3
            ProtocolLog.info(true, identifier, messageTypeId, uniqueId, msgConfirmation?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(msgConfirmation))
            meterRegistry.counter("se.in", "type", DataTransferConfirmation.simpleName).increment()
            publishMetricsCounterIncrement("se.in", ["type": DataTransferConfirmation.simpleName], 1)

            if (msgConfirmation.status != DataTransferStatus.Accepted) {
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "DataTransferError", objectMapper.writeValueAsString(msgConfirmation))
            }
        }

        connections.pushCommandWithDelay(UUID.fromString(sessionId), request, onSuccess, 0, uniqueId)
    }

    private ZonedDateTime parse(String dateString) {
        try {
            DateTimeFormatter f = DateTimeFormatter.ofPattern(edgeConfig.dateFormatString)

            ZonedDateTime zdt = ZonedDateTime.parse(dateString, f);
            return zdt
        } catch (Exception e) {
            logger.error("Unable to parse date", e)
        }
        return null
    }

    @Topic("ChargerCredsUpdated")
    void chargerCredsUpdated(@KafkaKey String bucketId, String payload) {
        logger.info("Request to update charger credentials received from the backend")
        authCacheService.invalidateCache()
    }

    protected void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
        def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
        if(kafkaEnabled)
            messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
    }

    /**
     * Validates ChargingProfile for RemoteStartTransaction-specific constraints per OCPP 1.6 §7.4.15.
     * Leverages ChargingProfile.validate() for standard OCPP validation, then adds business logic checks.
     * Returns null if valid, or a String error reason if invalid.
     *
     * US-RCP102-01: Remote Start Transaction with Charging Profile
     * [OCPP-COMPLIANCE] §3.13, §7.4.15
     *
     * @param profile The ChargingProfile from the Kafka payload
     * @param connectorId The connectorId (connectorIndex) from the RemoteStartTransaction command
     * @return null if valid, error message string if invalid
     */
    protected String validateTxProfile(ChargingProfile profile, Integer connectorId) {
        // Step 1: Use built-in OCPP validation (validates all required fields, stackLevel >= 0, nested structures)
        if (!profile.validate()) {
            return "ChargingProfile failed OCPP validation - check required fields (chargingProfileId, stackLevel, chargingProfilePurpose, chargingProfileKind, chargingSchedule)"
        }
        
        // Step 2: RemoteStartTransaction-specific business logic (OCPP 1.6 §7.4.15)
        // Per spec: "The Charging Profile...SHALL have chargingProfilePurpose set to TxProfile"
        if (profile.chargingProfilePurpose != ChargingProfilePurposeType.TxProfile) {
            return "Invalid chargingProfilePurpose: ${profile.chargingProfilePurpose} — RemoteStartTransaction requires TxProfile"
        }
        
        // Per spec: TxProfile requires a specific connector (connectorId > 0)
        if (!connectorId || connectorId <= 0) {
            return "connectorId must be > 0 when chargingProfile with TxProfile is present, got: ${connectorId}"
        }
        
        return null
    }
}
