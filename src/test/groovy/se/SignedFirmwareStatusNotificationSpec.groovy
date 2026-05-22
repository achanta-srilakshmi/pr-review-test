package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationConfirmation
import eu.chargetime.ocpp.model.securityext.SignedFirmwareStatusNotificationRequest
import eu.chargetime.ocpp.model.securityext.types.FirmwareStatusEnumType
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

/**
 * Unit tests for US-LOGFIRM107-02 Step 1: SignedFirmwareStatusNotification full handler in SecurityExtEH
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
 * [OCA-CERT-RELEVANT] TC-62, TC-63
 */
class SignedFirmwareStatusNotificationSpec extends Specification {

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

    // TC-LOGFIRM107-02-014: Installed — terminal success
    def "SignedFirmwareStatusNotification Installed — publishes to Kafka, INFO log"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.Installed)
        request.requestId = 1

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", "CP001", "SignedFirmwareStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.status == "Installed" &&
            body.requestId == 1
        })
        0 * incoming.securityEvent(_, _, _, _)
        result instanceof SignedFirmwareStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-02-015: InvalidSignature — dual publish (TC-63)
    def "SignedFirmwareStatusNotification InvalidSignature — publishes to SignedUpdateFirmwareResponse AND SecurityEvent"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.InvalidSignature)
        request.requestId = 1

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then: "Published to SignedUpdateFirmwareResponse"
        1 * incoming.signedUpdateFirmwareResponse("000", "CP001", "SignedFirmwareStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "InvalidSignature" && body.requestId == 1
        })

        and: "CSMS-correlated event published to SecurityEvent"
        1 * incoming.securityEvent("000", "CP001", "InvalidSignature", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.type == "InvalidSignature" &&
            body.requestId == 1 &&
            body.source == "CSMS"
        })

        and:
        result instanceof SignedFirmwareStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-02-010: Downloading — progress, no SecurityEvent
    def "SignedFirmwareStatusNotification Downloading — publishes to Kafka, no SecurityEvent"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.Downloading)
        request.requestId = 1

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", "CP001", "SignedFirmwareStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Downloading"
        })
        0 * incoming.securityEvent(_, _, _, _)
        result instanceof SignedFirmwareStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-02-016: DownloadFailed
    def "SignedFirmwareStatusNotification DownloadFailed — publishes, no SecurityEvent"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.DownloadFailed)
        request.requestId = 1

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", "CP001", "SignedFirmwareStatusNotification", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "DownloadFailed"
        })
        0 * incoming.securityEvent(_, _, _, _)
        result instanceof SignedFirmwareStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-02-021: Kafka fails, still returns confirmation
    def "SignedFirmwareStatusNotification — Kafka fails, still returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.Installed)
        request.requestId = 1
        incoming.signedUpdateFirmwareResponse(*_) >> { throw new RuntimeException("Kafka down") }

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        result.class == SignedFirmwareStatusNotificationConfirmation
        noExceptionThrown()
    }

    // TC-LOGFIRM107-02-022: Unknown session
    def "SignedFirmwareStatusNotification — unknown session still returns confirmation"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> null
        def request = new SignedFirmwareStatusNotificationRequest(FirmwareStatusEnumType.Installed)

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", null, "SignedFirmwareStatusNotification", _)
        result.class == SignedFirmwareStatusNotificationConfirmation
    }

    // TC-LOGFIRM107-02-023: All 14 FirmwareStatusEnumType values
    @Unroll
    def "All FirmwareStatusEnumType values handled: #status"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-ALL"
        def request = new SignedFirmwareStatusNotificationRequest(status)
        request.requestId = 99

        when:
        def result = handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        1 * incoming.signedUpdateFirmwareResponse(_, _, "SignedFirmwareStatusNotification", _)
        result.class == SignedFirmwareStatusNotificationConfirmation

        where:
        status << FirmwareStatusEnumType.values()
    }

    // Verify InvalidSignature is the ONLY status that triggers SecurityEvent
    @Unroll
    def "Only InvalidSignature triggers SecurityEvent, not #status"() {
        given:
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-SEC"
        def request = new SignedFirmwareStatusNotificationRequest(status)
        request.requestId = 1

        when:
        handler.handleSignedFirmwareStatusNotificationRequest(sessionId, request)

        then:
        0 * incoming.securityEvent(_, _, _, _)

        where:
        status << FirmwareStatusEnumType.values().findAll { it != FirmwareStatusEnumType.InvalidSignature }
    }
}

