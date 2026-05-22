package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.securityext.GetLogConfirmation
import eu.chargetime.ocpp.model.securityext.GetLogRequest
import eu.chargetime.ocpp.model.securityext.types.LogStatusEnumType
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-LOGFIRM107-01 Step 2: GetLog sender in OcppEventBroadcaster
 *
 * Tests the handleGetLog() method invoked via getLog() Kafka consumer.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1 — UC04 GetLog
 * [OCA-CERT-RELEVANT] TC-61
 */
class GetLogSenderSpec extends Specification {

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

    // TC-LOGFIRM107-01-001
    def "GetLog SecurityLog — Accepted"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts GetLog"
        def confirmation = new GetLogConfirmation(LogStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload/CP001"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        1 * incoming.getLogResponse("000", identifier, "GetLogResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.logType == "SecurityLog" &&
            body.requestId == 1 &&
            body.status == "Accepted"
        })
    }

    // TC-LOGFIRM107-01-002
    def "GetLog DiagnosticsLog — Accepted (logType-agnostic)"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new GetLogConfirmation(LogStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            logType: "DiagnosticsLog",
            requestId: 2,
            remoteLocation: "https://logs.example.com/upload/CP001"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        1 * incoming.getLogResponse("000", identifier, "GetLogResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.logType == "DiagnosticsLog" &&
            body.status == "Accepted"
        })
    }

    // TC-LOGFIRM107-01-003
    def "GetLog with optional timestamps"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        GetLogRequest capturedRequest = null
        def confirmation = new GetLogConfirmation(LogStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> { args ->
            capturedRequest = args[1] as GetLogRequest
            return confirmation
        }

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload/CP001",
            oldestTimestamp: "2026-04-01T00:00:00Z",
            latestTimestamp: "2026-04-16T23:59:59Z"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        capturedRequest != null
        capturedRequest.log.oldestTimestamp != null
        capturedRequest.log.latestTimestamp != null
    }

    // TC-LOGFIRM107-01-004
    def "GetLog with retries and retryInterval"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        GetLogRequest capturedRequest = null
        def confirmation = new GetLogConfirmation(LogStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> { args ->
            capturedRequest = args[1] as GetLogRequest
            return confirmation
        }

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload/CP001",
            retries: 3,
            retryInterval: 30
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        capturedRequest != null
        capturedRequest.retries == 3
        capturedRequest.retryInterval == 30
    }

    // TC-LOGFIRM107-01-005
    def "GetLog — Rejected by charge point"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new GetLogConfirmation(LogStatusEnumType.Rejected)
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload/CP001"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        1 * incoming.getLogResponse("000", identifier, "GetLogResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Rejected"
        })
    }

    // TC-LOGFIRM107-01-006
    def "GetLog — Charge point not connected"() {
        given:
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then: "Error response with ChargePointNotConnected"
        1 * incoming.getLogResponse("000", identifier, "GetLogResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "ChargePointNotConnected"
        })

        and: "No OCPP request sent"
        0 * connections.callInline(_, _)
    }

    // TC-LOGFIRM107-01-007
    def "GetLog — OCPP call throws exception"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> {
            throw new RuntimeException("OCPP timeout")
        }

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then:
        1 * incoming.getLogResponse("000", identifier, "GetLogResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Error" &&
            body.error != null
        })
    }

    // TC-LOGFIRM107-01-008
    def "GetLog — NotConnectedException silently discarded"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.callInline(UUID.fromString(sessionId), _ as GetLogRequest) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([
            logType: "SecurityLog",
            requestId: 1,
            remoteLocation: "https://logs.example.com/upload"
        ])

        when:
        broadcaster.getLog("000", identifier, payload)

        then: "No response published"
        0 * incoming.getLogResponse(_, _, _, _)
    }

    // TC-LOGFIRM107-01-018: Existing eventTypes still work
    def "Existing InstallCertificate eventType still routes correctly after GetLog addition"() {
        given:
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        def confirmation = new eu.chargetime.ocpp.model.securityext.InstallCertificateConfirmation(
            eu.chargetime.ocpp.model.securityext.types.CertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as eu.chargetime.ocpp.model.securityext.InstallCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            certificateType: "ManufacturerRootCertificate",
            certificate: "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"
        ])

        when:
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then:
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted"
        })
    }
}

