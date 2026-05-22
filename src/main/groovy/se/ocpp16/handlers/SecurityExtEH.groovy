package se.ocpp16.handlers

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.feature.profile.securityext.ServerSecurityExtEventHandler
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationRequest
import eu.chargetime.ocpp.model.securityext.SignCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.SignCertificateRequest
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.types.GenericStatusEnumType
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.OcppEventReceiver
import se.service.ConnectionService

import java.time.ZoneOffset
import java.time.ZonedDateTime

/**
 * Handler for OCPP 1.6 SecurityExt profile — CP-initiated messages.
 *
 * US-CERT105-01: Stub implementations for LogStatusNotification, SignCertificate,
 * SignedFirmwareStatusNotification.
 * US-CERT105-02: Full SecurityEventNotification handler implementation.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4, §5 — SecurityExt profile
 */
@Singleton
class SecurityExtEH implements ServerSecurityExtEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(SecurityExtEH.class)

    private final ConnectionService connections
    private final OcppEventReceiver incoming
    private final ObjectMapper objectMapper
    private final EdgeConfig edgeConfig

    boolean kafkaEnabled = true

    /**
     * Constructor with full dependency injection for SecurityEventNotification support.
     * US-CERT105-02: Inject ConnectionService, OcppEventReceiver, ObjectMapper, EdgeConfig.
     */
    @Inject
    SecurityExtEH(ConnectionService connections,
                  OcppEventReceiver incoming,
                  ObjectMapper objectMapper,
                  EdgeConfig edgeConfig) {
        this.connections = connections
        this.incoming = incoming
        this.objectMapper = objectMapper
        this.edgeConfig = edgeConfig
    }

    /**
     * CP-initiated: SecurityEventNotification — full handler (US-CERT105-02).
     *
     * Resolves charger identity from session, publishes event to SecurityEvent Kafka topic,
     * logs WARN for InvalidCentralSystemCertificate (INFO for all others),
     * and returns empty confirmation. Non-blocking — OCPP acknowledgement does not
     * depend on Kafka availability.
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §5 — SecurityEventNotification
     * [OCA-CERT-RELEVANT] TC-60
     */
    @Override
    SecurityEventNotificationConfirmation handleSecurityEventNotificationRequest(UUID sessionId, SecurityEventNotificationRequest request) {
        def chargePointId = connections?.getChargerId(sessionId?.toString())

        def event = [
            chargePointId: chargePointId,
            type         : request.type,
            timestamp    : request.timestamp?.toString(),
            techInfo     : request.techInfo,
            receivedAt   : ZonedDateTime.now(ZoneOffset.UTC).toString()
        ]

        try {
            def jsonPayload = objectMapper.writeValueAsString(event)
            if(kafkaEnabled)
                incoming.securityEvent(edgeConfig.kafkaNodeId, chargePointId, "SecurityEventNotification", jsonPayload)
        } catch (Exception e) {
            logger.warn("Failed to publish SecurityEvent to Kafka for {} — {}: {}", chargePointId, request.type, e.message)
        }

        if ("InvalidCentralSystemCertificate" == request.type) {
            logger.warn("Security event from {}: {} at {} — {}", chargePointId, request.type, request.timestamp, request.techInfo)
        } else {
            logger.info("Security event from {}: {} at {}", chargePointId, request.type, request.timestamp)
        }

        return new SecurityEventNotificationConfirmation()
    }

    /**
     * CP-initiated: LogStatusNotification — full handler (US-LOGFIRM107-01).
     *
     * Resolves charger identity from session, publishes status to GetLogResponse
     * Kafka topic, and returns empty confirmation. Non-blocking — OCPP acknowledgement does not
     * depend on Kafka availability.
     *
     * Log levels: INFO for terminal success (Uploaded), WARN for failures
     * (UploadFailure, PermissionDenied, BadMessage, NotSupportedOperation), DEBUG for in-progress (Uploading, Idle).
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1 — UC04 GetLog
     * [OCA-CERT-RELEVANT] TC-61
     */
    @Override
    LogStatusNotificationConfirmation handleLogStatusNotificationRequest(UUID sessionId, LogStatusNotificationRequest request) {
        def chargePointId = connections?.getChargerId(sessionId?.toString())
        def status = request?.status?.toString()
        def requestId = request?.requestId

        def event = [
            chargePointId: chargePointId,
            status       : status,
            requestId    : requestId,
            timestamp    : ZonedDateTime.now(ZoneOffset.UTC).toString()
        ]

        try {
            def jsonPayload = objectMapper.writeValueAsString(event)
            if(kafkaEnabled)
                incoming.getLogResponse(edgeConfig.kafkaNodeId, chargePointId,
                "LogStatusNotification", jsonPayload)
        } catch (Exception e) {
            logger.warn("Failed to publish LogStatusNotification to Kafka for {} — {}", chargePointId, e.message)
        }

        switch (status) {
            case "Uploaded":
                logger.info("LogStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
                break
            case "UploadFailure":
            case "PermissionDenied":
            case "BadMessage":
            case "NotSupportedOperation":
                logger.warn("LogStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
                break
            default:
                logger.debug("LogStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
        }

        return new LogStatusNotificationConfirmation()
    }

    /**
     * CP-initiated: SignCertificate — full handler (US-CERT106-02).
     *
     * Validates the CSR (non-null, non-empty, contains PEM marker), resolves charger
     * identity from session, publishes CSR to CertificateManagementResponse Kafka topic
     * for admin-service to sign. Returns Accepted if CSR is valid, Rejected if malformed.
     * Non-blocking — OCPP acknowledgement does not depend on Kafka availability.
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3 — Step 2: CP delivers CSR
     * [OCA-CERT-RELEVANT] TC-68
     */
    @Override
    SignCertificateConfirmation handleSignCertificateRequest(UUID sessionId, SignCertificateRequest request) {
        def chargePointId = connections?.getChargerId(sessionId?.toString())
        def csr = request?.getCsr()

        // Validate CSR: null, empty, or missing PEM marker → reject
        if (!csr || !csr.contains("BEGIN CERTIFICATE REQUEST")) {
            logger.warn("Malformed or empty CSR from {} (session {})", chargePointId, sessionId)
            return new SignCertificateConfirmation(GenericStatusEnumType.Rejected)
        }

        // Publish CSR to Kafka for admin-service to sign
        try {
            def event = [
                chargePointId: chargePointId,
                csr          : csr,
                timestamp    : java.time.Instant.now().toString()
            ]
            def jsonPayload = objectMapper.writeValueAsString(event)
            if(kafkaEnabled)
                incoming.certificateManagementResponse(edgeConfig.kafkaNodeId, chargePointId,
                "SignCertificateRequest", jsonPayload)
        } catch (Exception e) {
            logger.warn("Failed to publish SignCertificate CSR to Kafka for {} — {}", chargePointId, e.message)
        }

        logger.info("SignCertificate accepted from {} — CSR published to Kafka", chargePointId)
        return new SignCertificateConfirmation(GenericStatusEnumType.Accepted)
    }

    /**
     * CP-initiated: SignedFirmwareStatusNotification — full handler (US-LOGFIRM107-02).
     *
     * Resolves charger identity from session, publishes status to SignedUpdateFirmwareResponse
     * Kafka topic. On InvalidSignature, additionally publishes CSMS-correlated event to SecurityEvent
     * topic. Non-blocking — OCPP acknowledgement does not depend on Kafka availability.
     *
     * Log levels: INFO for Installed (terminal success), WARN for failures
     * (InvalidSignature, DownloadFailed, InstallationFailed, InstallVerificationFailed, Idle),
     * DEBUG for progress (Downloading, Downloaded, SignatureVerified, Installing, etc.).
     *
     * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
     * [OCA-CERT-RELEVANT] TC-62, TC-63
     */
    @Override
    SignedFirmwareStatusNotificationConfirmation handleSignedFirmwareStatusNotificationRequest(UUID sessionId, SignedFirmwareStatusNotificationRequest request) {
        def chargePointId = connections?.getChargerId(sessionId?.toString())
        def status = request?.status?.toString()
        def requestId = request?.requestId

        def event = [
            chargePointId: chargePointId,
            status       : status,
            requestId    : requestId,
            timestamp    : ZonedDateTime.now(ZoneOffset.UTC).toString()
        ]

        try {
            def jsonPayload = objectMapper.writeValueAsString(event)
            if(kafkaEnabled)
                incoming.signedUpdateFirmwareResponse(edgeConfig.kafkaNodeId, chargePointId,
                "SignedFirmwareStatusNotification", jsonPayload)
        } catch (Exception e) {
            logger.warn("Failed to publish SignedFirmwareStatusNotification to Kafka for {} — {}", chargePointId, e.message)
        }

        // InvalidSignature: additionally publish CSMS-correlated SecurityEvent
        if ("InvalidSignature" == status) {
            try {
                def secEvent = [
                    chargePointId: chargePointId,
                    type         : "InvalidSignature",
                    requestId    : requestId,
                    source       : "CSMS",
                    timestamp    : ZonedDateTime.now(ZoneOffset.UTC).toString()
                ]
                if(kafkaEnabled)
                    incoming.securityEvent(edgeConfig.kafkaNodeId, chargePointId,
                    "InvalidSignature", objectMapper.writeValueAsString(secEvent))
            } catch (Exception e) {
                logger.warn("Failed to publish InvalidSignature SecurityEvent for {} — {}", chargePointId, e.message)
            }
        }

        switch (status) {
            case "Installed":
                logger.info("SignedFirmwareStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
                break
            case "InvalidSignature":
            case "DownloadFailed":
            case "InstallationFailed":
            case "InstallVerificationFailed":
            case "Idle":
                logger.warn("SignedFirmwareStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
                break
            default:
                logger.debug("SignedFirmwareStatusNotification from {}: {} (requestId: {})", chargePointId, status, requestId)
        }

        return new SignedFirmwareStatusNotificationConfirmation()
    }
}

