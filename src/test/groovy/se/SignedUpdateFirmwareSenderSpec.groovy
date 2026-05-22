package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareConfirmation
import eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareRequest
import eu.chargetime.ocpp.model.securityext.types.UpdateFirmwareStatusEnumType
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-LOGFIRM107-02 Step 2: SignedUpdateFirmware sender in OcppEventBroadcaster
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.5 — UC08 SignedUpdateFirmware
 * [OCA-CERT-RELEVANT] TC-62, TC-63
 */
class SignedUpdateFirmwareSenderSpec extends Specification {

    @Subject
    OcppEventBroadcaster broadcaster

    ConnectionService connections = Mock()
    OcppEventReceiver incoming = Mock()
    MeterRegistry meterRegistry = Mock()
    MessageProducer messageProducer = Mock()
    EdgeConfig edgeConfig = Mock()
    ObjectMapper objectMapper = new ObjectMapper()
    Counter mockCounter = Mock()

    def setup() {
        edgeConfig.kafkaNodeId >> "000"
        meterRegistry.counter(_, _, _) >> mockCounter

        broadcaster = new OcppEventBroadcaster()
        broadcaster.connections = connections
        broadcaster.incoming = incoming
        broadcaster.meterRegistry = meterRegistry
        broadcaster.messageProducer = messageProducer
        broadcaster.edgeConfig = edgeConfig
        broadcaster.objectMapper = objectMapper
    }

    private String buildPayload(Map overrides = [:]) {
        def base = [
            requestId: 1,
            firmware: [
                location: "https://firmware.example.com/v2.1.0.bin",
                retrieveDateTime: "2026-04-17T02:00:00Z",
                signingCertificate: "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
                signature: "MEUCIQC...base64..."
            ]
        ]
        if (overrides) base.putAll(overrides)
        return objectMapper.writeValueAsString(base)
    }

    // TC-LOGFIRM107-02-001
    def "SignedUpdateFirmware — Accepted"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> confirmation

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.requestId == 1 &&
            body.status == "Accepted"
        })
    }

    // TC-LOGFIRM107-02-002
    def "SignedUpdateFirmware — Rejected"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.Rejected)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> confirmation

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Rejected"
        })
    }

    // TC-LOGFIRM107-02-003
    def "SignedUpdateFirmware — InvalidCertificate"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.InvalidCertificate)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> confirmation

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "InvalidCertificate"
        })
    }

    // TC-LOGFIRM107-02-004
    def "SignedUpdateFirmware — RevokedCertificate"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.RevokedCertificate)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> confirmation

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "RevokedCertificate"
        })
    }

    // TC-LOGFIRM107-02-005
    def "SignedUpdateFirmware — AcceptedCanceled"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.AcceptedCanceled)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> confirmation

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "AcceptedCanceled"
        })
    }

    // TC-LOGFIRM107-02-006
    def "SignedUpdateFirmware with optional installDateTime, retries, retryInterval"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        SignedUpdateFirmwareRequest capturedRequest = null
        def confirmation = new SignedUpdateFirmwareConfirmation(UpdateFirmwareStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> { args ->
            capturedRequest = args[1] as SignedUpdateFirmwareRequest
            return confirmation
        }

        def payload = objectMapper.writeValueAsString([
            requestId: 1,
            retries: 3,
            retryInterval: 60,
            firmware: [
                location: "https://firmware.example.com/v2.1.0.bin",
                retrieveDateTime: "2026-04-17T02:00:00Z",
                installDateTime: "2026-04-17T03:00:00Z",
                signingCertificate: "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
                signature: "MEUCIQC...base64..."
            ]
        ])

        when:
        broadcaster.signedUpdateFirmware("000", identifier, payload)

        then:
        capturedRequest != null
        capturedRequest.retries == 3
        capturedRequest.retryInterval == 60
        capturedRequest.firmware.installDateTime != null
    }

    // TC-LOGFIRM107-02-007
    def "SignedUpdateFirmware — charge point not connected"() {
        given:
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "ChargePointNotConnected"
        })
        0 * connections.callInline(_, _)
    }

    // TC-LOGFIRM107-02-008
    def "SignedUpdateFirmware — OCPP exception"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> {
            throw new RuntimeException("OCPP timeout")
        }

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        1 * incoming.signedUpdateFirmwareResponse("000", identifier, "SignedUpdateFirmwareResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Error" && body.error != null
        })
    }

    // TC-LOGFIRM107-02-009
    def "SignedUpdateFirmware — NotConnectedException silently discarded"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.callInline(UUID.fromString(sessionId), _ as SignedUpdateFirmwareRequest) >> {
            throw new NotConnectedException()
        }

        when:
        broadcaster.signedUpdateFirmware("000", identifier, buildPayload())

        then:
        0 * incoming.signedUpdateFirmwareResponse(_, _, _, _)
    }
}
