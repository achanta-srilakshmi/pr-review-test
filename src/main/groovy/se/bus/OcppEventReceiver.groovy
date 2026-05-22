package se.bus

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.MessageHeader

@KafkaClient
interface OcppEventReceiver {
    /**
     *
     * @param bucketId '000' By default
     * @param sessionId
     * @param eventType One of "BootNotification", "StatusNotification", "NewSession" or "LostSession"
     * @param payload Always a JSON string. Empty JSON ("{}") in case of LostSession and '{"identifier":"XYZ"}' in case of "NewSession"
     */
    @Topic("BootCycle")
    void bootCycle(@KafkaKey String bucketId, @MessageHeader("sessionId") String sessionId,
                   @MessageHeader("identifier") String identifier, @MessageHeader("eventType") String eventType,
                   String payload)

    /**
     *
     * @param bucketId '000' By default
     * @param sessionId
     * @param payload Always a JSON string.
     *
     */
    @Topic("BootNotification")
    void bootNotification(@KafkaKey String bucketId, @MessageHeader("sessionId") String sessionId,
                   @MessageHeader("identifier") String identifier,
                   String payload)

    /**
     *
     * @param bucketId '000' By default
     * @param sessionId
     * @param eventType One of "StartTransaction", "StopTransaction", or "MeterValues"
     * @param payload Always a JSON string.
     */
    @Topic("OCPPTransaction")
    void ocppTransaction(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                         @MessageHeader("eventType") String eventType,
                         @MessageHeader("qrCode") String qrCode, String payload)
    @Topic("ChargerMeterValues")
    void chargerMeterValues(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                         @MessageHeader("qrCode") String qrCode, String payload)

    @Topic("AuthorizeRequest")
    void authorize(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                   String payload)

    @Topic("Heartbeat")
    void heartbeat(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                   String payload)

    @Topic("DiagnosticsConfirmation")
    void diagnosticsConfirmed(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                              String payload)

    @Topic("DiagnosticsLogStatus")
    void diaglosticsLogStatus(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                              String payload)

    @Topic("FirmwareStatus")
    void firmwareStatus(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                        String payload)

    @Topic("DataTransfer")
    void dataTransfer(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                      String payload)

    @Topic("ChargerConfiguration")
    void chargerConfiguration(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                              @MessageHeader("eventType") String eventType, String payload)

    /**
     *
     * @param bucketId
     * @param sessionId
     * @param eventType : One of the request types invoking which the error was returned
     * @param payload : json of the error message
     */
    @Topic("ChargerError")
    void chargerError(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                      @MessageHeader("eventType") String eventType, String payload)
    @Topic("ConfigurationUpdate")
    void configurationUpdate(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier, String payload)
    @Topic("ChargingProfileResponse")
    void chargingProfile(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                         @MessageHeader("eventType") String eventType, String payload)

    @Topic("InactiveCharger")
    void inactiveCharger(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                         @MessageHeader("eventType") String eventType, String payload)

    /**
     * Publishes certificate management command responses (InstallCertificate, DeleteCertificate,
     * GetInstalledCertificateIds) to be consumed by admin-service.
     *
     * US-CERT105-01: SecurityExt Certificate Management Command Handlers
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param eventType One of: InstallCertificateResponse, DeleteCertificateResponse, GetInstalledCertificateIdsResponse
     * @param payload JSON response body
     */
    @Topic("CertificateManagementResponse")
    void certificateManagementResponse(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                       @MessageHeader("eventType") String eventType, String payload)

    /**
     * Publishes GetLog command responses to be consumed by admin-service.
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1 — UC04 GetLog
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param eventType Always "GetLogResponse"
     * @param payload JSON response body
     */
    @Topic("GetLogResponse")
    void getLogResponse(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                        @MessageHeader("eventType") String eventType, String payload)

    /**
     * Publishes SignedUpdateFirmware command responses to be consumed by admin-service.
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param eventType Always "SignedUpdateFirmwareResponse"
     * @param payload JSON response body
     */
    @Topic("SignedUpdateFirmwareResponse")
    void signedUpdateFirmwareResponse(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                                      @MessageHeader("eventType") String eventType, String payload)

    /**
     * Publishes security events from charge points (e.g., InvalidCentralSystemCertificate)
     * to be consumed by future alerting/audit services.
     *
     * US-CERT105-02: SecurityEventNotification Handler
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §5
     *
     * @param bucketId Kafka partition key (edgeConfig.kafkaNodeId)
     * @param identifier Charge point identifier
     * @param eventType Always "SecurityEventNotification" for this topic
     * @param payload JSON event body containing chargePointId, type, timestamp, techInfo, receivedAt
     */
    @Topic("SecurityEvent")
    void securityEvent(@KafkaKey String bucketId, @MessageHeader("identifier") String identifier,
                       @MessageHeader("eventType") String eventType, String payload)

    /**
     * Publishes a reservation command result to the ReservationResponse Kafka topic.
     * Consumed by charge-session-monitor.
     * OCPP 1.6 §3.11 (ReserveNow), §3.3 (CancelReservation)
     *
     * @param bucketId   Kafka message key — edgeConfig.kafkaNodeId
     * @param identifier charger identifier (e.g. "EVK-001")
     * @param eventType  "ReserveNow" or "CancelReservation"
     * @param payload    JSON string: {status, reservationId}
     */
    @Topic("ReservationResponse")
    void reservationResponse(@KafkaKey String bucketId,
                             @MessageHeader("identifier") String identifier,
                             @MessageHeader("eventType") String eventType,
                             String payload)

    /**
     * Publishes the result of an UnlockConnector command to downstream CSMS services.
     * [OCPP-COMPLIANCE] OCPP 1.6 §6.21 (UnlockConnector)
     *
     * Published on successful unlock (status == Unlocked).
     * Failed unlocks (UnlockFailed, NotSupported) are published to ChargerError instead.
     *
     * @param bucketId  Kafka partition key — edgeConfig.kafkaNodeId
     * @param identifier Charge point identifier
     * @param eventType  Always "UnlockConnectorResponse" for this topic
     * @param payload   JSON string: {"status":"Unlocked"}
     */
    @Topic("UnlockConnectorResponse")
    void unlockConnectorResponse(@KafkaKey String bucketId,
                                 @MessageHeader("identifier") String identifier,
                                 @MessageHeader("eventType") String eventType,
                                 String payload)
}
