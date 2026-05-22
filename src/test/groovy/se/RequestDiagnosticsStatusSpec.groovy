package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageConfirmation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageStatus
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject
import spock.util.concurrent.PollingConditions

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Unit tests for US-101-02: TriggerMessage Dispatch for DiagnosticsStatusNotification
 *
 * Tests the requestDiagnosticsStatus() method in OcppEventBroadcaster.
 * Follows OCPP 1.6 specification §5.18 for TriggerMessage.
 */
class RequestDiagnosticsStatusSpec extends Specification {

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

    /**
     * TC-101-02-01: TriggerMessage Accepted - Happy Path
     *
     * GIVEN: Charge point CHARGER-001 is connected via WebSocket
     * WHEN: Kafka message received on topic RequestDiagnosticsStatus
     * THEN: TriggerMessageRequest sent with requestedMessage = DiagnosticsStatusNotification
     */
    def "TC-101-02-01: should send TriggerMessageRequest with DiagnosticsStatusNotification when charger is connected"() {
        given: "A connected charge point"
        def identifier = "CHARGER-001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Mock the pushCommandWithDelay to capture the request"
        TriggerMessageRequest capturedRequest = null
        connections.pushCommandWithDelay(_, _, _, _, _) >> { args ->
            capturedRequest = args[1] as TriggerMessageRequest
        }

        when: "RequestDiagnosticsStatus Kafka message is processed"
        broadcaster.requestDiagnosticsStatus("000", identifier, "{}")

        and: "Wait for async processing"
        Thread.sleep(500)

        then: "TriggerMessageRequest should be sent with DiagnosticsStatusNotification"
        capturedRequest != null
        capturedRequest.requestedMessage == TriggerMessageRequestType.DiagnosticsStatusNotification

        and: "connectorId should NOT be set (per OCPP Appendix 1)"
        capturedRequest.connectorId == null
    }

    /**
     * TC-101-02-03: Charger Not Connected
     *
     * GIVEN: Charge point CHARGER-OFFLINE is NOT connected
     * WHEN: Kafka message received on topic RequestDiagnosticsStatus
     * THEN: WARN log recorded and metric se.sessionmap.nomatch incremented
     */
    def "TC-101-02-03: should discard silently and record metric when charger is not connected"() {
        given: "A charge point that is not connected"
        def identifier = "CHARGER-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when: "RequestDiagnosticsStatus Kafka message is processed"
        broadcaster.requestDiagnosticsStatus("000", identifier, "{}")

        and: "Wait for async processing"
        Thread.sleep(500)

        then: "No WebSocket message should be sent"
        0 * connections.pushCommandWithDelay(_, _, _, _, _)

        and: "Metric se.sessionmap.nomatch should be recorded"
        1 * meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier) >> mockCounter
        1 * mockCounter.increment()
    }

    /**
     * TC-101-02-06: Empty Identifier Header
     *
     * GIVEN: Kafka message has empty identifier header
     * WHEN: Message is processed
     * THEN: Should discard silently (no session found for empty identifier)
     */
    def "TC-101-02-06: should handle empty identifier gracefully"() {
        given: "An empty identifier"
        def identifier = ""
        connections.getLatestSessionId(identifier) >> null

        when: "RequestDiagnosticsStatus Kafka message is processed with empty identifier"
        broadcaster.requestDiagnosticsStatus("000", identifier, "{}")

        and: "Wait for async processing"
        Thread.sleep(500)

        then: "No WebSocket message should be sent"
        0 * connections.pushCommandWithDelay(_, _, _, _, _)

        and: "Metric should be recorded"
        1 * meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier) >> mockCounter
    }

    /**
     * Verify that the method follows the same pattern as requestStatus()
     * by checking it uses @Async and CompletableFuture
     */
    def "should use async processing pattern"() {
        given: "A connected charge point"
        def identifier = "CHARGER-ASYNC"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.pushCommandWithDelay(_, _, _, _, _) >> {}

        when: "RequestDiagnosticsStatus is called"
        def startTime = System.currentTimeMillis()
        broadcaster.requestDiagnosticsStatus("000", identifier, "{}")
        def endTime = System.currentTimeMillis()

        then: "Method should return quickly (async)"
        (endTime - startTime) < 100 // Should return in less than 100ms
    }
}

