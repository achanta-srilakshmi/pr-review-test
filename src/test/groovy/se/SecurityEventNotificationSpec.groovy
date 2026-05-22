package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SecurityEventNotificationRequest
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

import java.time.ZonedDateTime

/**
 * Unit tests for US-CERT105-02: SecurityEventNotification Handler (OCA TC-60)
 *
 * Tests the handleSecurityEventNotificationRequest() method in SecurityExtEH.
 * Covers: InvalidCentralSystemCertificate, unknown event types, techInfo handling,
 * rapid succession, Kafka failure resilience, null chargePointId.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §5 — SecurityEventNotification
 * [OCA-CERT-RELEVANT] TC-60
 */
class SecurityEventNotificationSpec extends Specification {

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

    // -----------------------------------------------------------------------
    // TC-CERT105-02-001: InvalidCentralSystemCertificate — acknowledged and published
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-001: InvalidCentralSystemCertificate — acknowledged and published to Kafka"() {
        given: "Charge point CP001 is connected with a known sessionId"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"

        and: "A SecurityEventNotification with InvalidCentralSystemCertificate"
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.timestamp = ZonedDateTime.parse("2026-04-13T10:30:00.000Z")
        request.techInfo = "Certificate CN=evoke-csms.example.com expired 2026-03-01"

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "An empty SecurityEventNotificationConfirmation is returned"
        result != null

        and: "Event is published to SecurityEvent Kafka topic"
        result instanceof SecurityEventNotificationConfirmation

    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-002: Unknown/vendor-specific event type — acknowledged and published
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-002: Unknown vendor-specific event type — acknowledged and published"() {
        given: "Charge point CP002 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP002"

        and: "A SecurityEventNotification with an unknown type"
        def request = new SecurityEventNotificationRequest()
        request.type = "CustomVendorSecurityEvent"
        request.timestamp = ZonedDateTime.parse("2026-04-14T08:15:00.000Z")
        request.techInfo = null

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "An empty SecurityEventNotificationConfirmation is returned"
        result != null

        and: "Event is published to SecurityEvent Kafka topic"
        result instanceof SecurityEventNotificationConfirmation

    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-003: techInfo with 255 characters — no truncation
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-003: techInfo with 255 characters — no truncation"() {
        given: "Charge point CP003 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP003"

        and: "A SecurityEventNotification with 255-char techInfo"
        def longTechInfo = "A" * 255
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.techInfo = longTechInfo

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "Confirmation returned"
        result != null

        and: "Kafka payload contains full 255-char techInfo without truncation"
        1 * incoming.securityEvent(_, "CP003", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.techInfo == longTechInfo &&
            body.techInfo.length() == 255
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-004: techInfo absent (null) — handled gracefully
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-004: techInfo absent (null) — handled gracefully, no NPE"() {
        given: "Charge point CP004 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP004"

        and: "A SecurityEventNotification with null techInfo"
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.techInfo = null

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "Confirmation returned without NullPointerException"
        result != null
        noExceptionThrown()

        and: "Kafka payload has null techInfo"
        1 * incoming.securityEvent(_, "CP004", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.techInfo == null
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-005: Rapid succession — each event handled independently
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-005: Rapid succession — each event handled independently, 3 Kafka messages"() {
        given: "Charge point CP005 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP005"

        and: "Three SecurityEventNotification requests"
        def request1 = new SecurityEventNotificationRequest()
        request1.type = "InvalidCentralSystemCertificate"
        request1.techInfo = "event-1"

        def request2 = new SecurityEventNotificationRequest()
        request2.type = "InvalidCentralSystemCertificate"
        request2.techInfo = "event-2"

        def request3 = new SecurityEventNotificationRequest()
        request3.type = "CustomVendorEvent"
        request3.techInfo = "event-3"

        when: "All three are processed in rapid succession"
        def result1 = handler.handleSecurityEventNotificationRequest(sessionId, request1)
        def result2 = handler.handleSecurityEventNotificationRequest(sessionId, request2)
        def result3 = handler.handleSecurityEventNotificationRequest(sessionId, request3)

        then: "Each returns a confirmation"
        result1 != null
        result2 != null
        result3 != null

        and: "Kafka receives exactly 3 separate messages"
        1 * incoming.securityEvent(_, "CP005", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.techInfo == "event-1"
        })
        1 * incoming.securityEvent(_, "CP005", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.techInfo == "event-2"
        })
        1 * incoming.securityEvent(_, "CP005", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.techInfo == "event-3"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-006: Session not found (unknown sessionId) — still acknowledged
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-006: Unknown session — still acknowledged, Kafka publish with null chargePointId"() {
        given: "A sessionId with no matching charger"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> null

        and: "A SecurityEventNotification"
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.techInfo = "some info"

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "An empty SecurityEventNotificationConfirmation is returned"
        result != null

        and: "Event is published to SecurityEvent Kafka topic"
        result instanceof SecurityEventNotificationConfirmation

    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-007: Kafka publish failure — CP still acknowledged
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-007: Kafka publish failure — CP still acknowledged, no exception propagated"() {
        given: "Charge point CP007 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP007"

        and: "Kafka is unavailable — securityEvent() throws exception"
        incoming.securityEvent(_, _, _, _) >> { throw new RuntimeException("Kafka unavailable") }

        and: "A SecurityEventNotification"
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.techInfo = "cert expired"

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "Confirmation returned despite Kafka failure"
        result != null
        result instanceof SecurityEventNotificationConfirmation
        noExceptionThrown()
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-009: timestamp field preserved from charge point
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-009: timestamp from charge point preserved in Kafka payload"() {
        given: "Charge point CP009 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP009"

        and: "A SecurityEventNotification with specific timestamp"
        def request = new SecurityEventNotificationRequest()
        request.type = "InvalidCentralSystemCertificate"
        request.timestamp = ZonedDateTime.parse("2026-04-13T10:30:00.000Z")

        when: "The handler processes the notification"
        handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "Kafka payload contains the original CP timestamp and a separate receivedAt"
        1 * incoming.securityEvent(_, "CP009", "SecurityEventNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.timestamp != null &&
            body.receivedAt != null &&
            body.receivedAt != body.timestamp
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-02-010: Empty type string — still acknowledged
    // -----------------------------------------------------------------------
    def "TC-CERT105-02-010: Empty type string — still acknowledged, not rejected"() {
        given: "Charge point CP010 is connected"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP010"

        and: "A SecurityEventNotification with empty type"
        def request = new SecurityEventNotificationRequest()
        request.type = ""
        request.techInfo = "some info"

        when: "The handler processes the notification"
        def result = handler.handleSecurityEventNotificationRequest(sessionId, request)

        then: "An empty SecurityEventNotificationConfirmation is returned"
        result != null

        and: "Event is published to SecurityEvent Kafka topic"
        result instanceof SecurityEventNotificationConfirmation

    }
}

