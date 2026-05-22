package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationRequest
import eu.chargetime.ocpp.model.securityext.SignCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.SignCertificateRequest
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.types.GenericStatusEnumType
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-CERT105-01 Step 1: SecurityExtEH handler class
 *
 * Tests the CP-initiated handler stub methods in SecurityExtEH.
 * SecurityEventNotification is fully tested in SecurityEventNotificationSpec (US-CERT105-02).
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4 — SecurityExt profile
 */
class SecurityExtEHSpec extends Specification {

    @Subject
    SecurityExtEH handler

    ConnectionService connections = Mock()
    OcppEventReceiver incoming = Mock()
    EdgeConfig edgeConfig = Mock()
    ObjectMapper objectMapper = new ObjectMapper()

    def setup() {
        edgeConfig.kafkaNodeId >> "000"
        handler = new SecurityExtEH(connections, incoming, objectMapper, edgeConfig)
    }

    def "handleSecurityEventNotificationRequest returns empty confirmation"() {
        given: "A SecurityEventNotificationRequest from a charge point"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-TEST"
        def request = new SecurityEventNotificationRequest()

        when: "The handler processes the request"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "A non-null SecurityEventNotificationConfirmation is returned"
        result != null
        result instanceof SecurityEventNotificationConfirmation
    }

    def "handleLogStatusNotificationRequest returns empty confirmation and publishes to Kafka (US-LOGFIRM107-01)"() {
        given: "A LogStatusNotificationRequest from a charge point"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-TEST"
        def request = new LogStatusNotificationRequest()

        when: "The handler processes the request"
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then: "A non-null LogStatusNotificationConfirmation is returned and event published"
        result != null
        result.class == LogStatusNotificationConfirmation
        1 * incoming.getLogResponse(_, _, "LogStatusNotification", _)
    }

    def "handleSignCertificateRequest returns Rejected status (stub — not implemented yet)"() {
        given: "A SignCertificateRequest from a charge point"
        def sessionId = UUID.randomUUID()
        def request = new SignCertificateRequest()

        when: "The handler processes the request"
        def result = handler.handleSignCertificateRequest(sessionId, request)

        then: "A SignCertificateConfirmation with Rejected status is returned"
        result != null
        result instanceof SignCertificateConfirmation
        result.status == GenericStatusEnumType.Rejected
    }

    def "handleSignedFirmwareStatusNotificationRequest returns confirmation and publishes to Kafka (US-LOGFIRM107-02)"() {
        given: "A SignedFirmwareStatusNotificationRequest from a charge point"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-TEST"
        def request = new SignedFirmwareStatusNotificationRequest()

        when: "The handler processes the request"
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then: "A non-null SignedFirmwareStatusNotificationConfirmation is returned"
        result != null
        result.class == SignedFirmwareStatusNotificationConfirmation
        1 * incoming.signedUpdateFirmwareResponse(_, _, "SignedFirmwareStatusNotification", _)
    }
}

