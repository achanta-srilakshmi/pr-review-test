package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.LogStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.types.UploadLogStatusEnumType
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

/**
 * Unit tests for US-LOGFIRM107-01 Step 1: LogStatusNotification full handler in SecurityExtEH
 *
 * Replaces the stub with a full implementation that publishes to GetLogResponse Kafka topic.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1 — UC04 GetLog
 * [OCA-CERT-RELEVANT] TC-61
 */
class LogStatusNotificationHandlerSpec extends Specification {

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

    // TC-LOGFIRM107-01-009
    def "LogStatusNotification Uploading — publishes to Kafka, returns confirmation"() {
        given: "A connected charge point sends LogStatusNotification with Uploading"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.Uploading)
        request.requestId = 1

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then: "Event published to GetLogResponse"
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.status == "Uploading" &&
            body.requestId == 1
        })

        and: "Returns non-null empty confirmation"
        result != null
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-010
    def "LogStatusNotification Uploaded — publishes to Kafka, returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.Uploaded)
        request.requestId = 1

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.status == "Uploaded" &&
            body.requestId == 1
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-011
    def "LogStatusNotification UploadFailure — publishes to Kafka, returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.UploadFailure)
        request.requestId = 1

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "UploadFailure"
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-012
    def "LogStatusNotification PermissionDenied — publishes to Kafka"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.PermissionDenied)
        request.requestId = 2

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "PermissionDenied"
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-013
    def "LogStatusNotification BadMessage — publishes to Kafka"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.BadMessage)

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "BadMessage"
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-014
    def "LogStatusNotification NotSupportedOperation — publishes to Kafka"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.NotSupportedOperation)

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "NotSupportedOperation"
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-015
    def "LogStatusNotification Idle — publishes to Kafka"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.Idle)

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", "CP001", "LogStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Idle"
        })
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-016
    def "LogStatusNotification — Kafka publish fails, still returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.Uploaded)
        request.requestId = 1

        and: "Kafka is unavailable"
        incoming.getLogResponse(_, _, _, _) >> { throw new RuntimeException("Kafka unavailable") }

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then: "Confirmation still returned (non-blocking)"
        result != null
        result instanceof LogStatusNotificationConfirmation
        noExceptionThrown()
    }

    // TC-LOGFIRM107-01-017
    def "LogStatusNotification — unknown session still returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> null
        def request = new LogStatusNotificationRequest(UploadLogStatusEnumType.Uploaded)

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse("000", null, "LogStatusNotification", _)
        result instanceof LogStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-01-019: Regression — SecurityEventNotification still works
    @Unroll
    def "All UploadLogStatusEnumType values are handled: #status"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-REGRESSION"
        def request = new LogStatusNotificationRequest(status)
        request.requestId = 99

        when:
        def result = handler.handleLogStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.getLogResponse(_, _, "LogStatusNotification", _)
        result != null
        LogStatusNotificationConfirmation.isInstance(result)

        where:
        status << UploadLogStatusEnumType.values()
    }
}

