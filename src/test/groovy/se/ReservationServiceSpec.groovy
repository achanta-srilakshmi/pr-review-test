package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.reservation.CancelReservationConfirmation
import eu.chargetime.ocpp.model.reservation.CancelReservationStatus
import eu.chargetime.ocpp.model.reservation.ReservationStatus
import eu.chargetime.ocpp.model.reservation.ReserveNowConfirmation
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.OCPPEventTypes
import se.service.ConnectionService
import se.service.ReservationService
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Spock unit tests for ReservationService.
 * Covers OCPP 1.6 §3.11 (ReserveNow) and §3.3 (CancelReservation).
 * All dependencies are mocked — no real Kafka, DB, or charger required.
 */
class ReservationServiceSpec extends Specification {

    static final String SESSION_ID = "1a2c7940-3f25-4d43-ac17-b6294420fea8"
    static final String IDENTIFIER = "EVK-001"
    static final String NODE_ID    = "test-node"

    ConnectionService connections   = Mock()
    OcppEventReceiver incoming      = Mock()
    EdgeConfig edgeConfig           = Stub(EdgeConfig) { getKafkaNodeId() >> NODE_ID }
    MessageProducer messageProducer = Stub(MessageProducer)
    MeterRegistry meterRegistry     = new SimpleMeterRegistry()
    ObjectMapper objectMapper       = new ObjectMapper().tap { findAndRegisterModules() }

    ReservationService reservationService = new ReservationService(
        objectMapper, edgeConfig, connections, incoming, meterRegistry, messageProducer
    )

    // ─────────────────────────────────────────────
    // RS-01 to RS-05 — reserveNow: all 5 ReservationStatus values
    // ─────────────────────────────────────────────

    @Unroll
    void "RS-#testId reserveNow publishes reservationResponse for status #status (BR-005, BR-006)"() {
        given: "charger session exists and returns given status"
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        def confirmation = new ReserveNowConfirmation()
        confirmation.status = status
        connections.callInline(UUID.fromString(SESSION_ID), _) >> confirmation

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 42
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then: "reservationResponse is published once with correct eventType and status"
        1 * incoming.reservationResponse(
            NODE_ID, IDENTIFIER, OCPPEventTypes.RESERVE_NOW_RESPONSE.toString(),
            { String p -> objectMapper.readValue(p, Map).status == status.toString() && objectMapper.readValue(p, Map).reservationId == 42 }
        )

        and: "chargerError is NOT published (BR-005 — non-Accepted is not an SE error)"
        0 * incoming.chargerError(_, _, _, _)

        where:
        testId | status
        "01"   | ReservationStatus.Accepted
        "02"   | ReservationStatus.Faulted
        "03"   | ReservationStatus.Occupied
        "04"   | ReservationStatus.Rejected
        "05"   | ReservationStatus.Unavailable
    }

    // ─────────────────────────────────────────────
    // RS-06 — reserveNow: null session → ChargerError (BR-007)
    // ─────────────────────────────────────────────

    void "RS-06 reserveNow with no active session publishes ChargerError with reservationId (BR-007, BR-008)"() {
        given:
        connections.getLatestSessionId("EVK-OFFLINE") >> null

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 99
        ])

        when:
        reservationService.reserveNow("EVK-OFFLINE", payload)

        then: "chargerError is published with eventType=ReserveNowError and reservationId in payload"
        1 * incoming.chargerError(
            NODE_ID, "EVK-OFFLINE", "ReserveNowError",
            { String p ->
                def body = objectMapper.readValue(p, Map)
                body.reservationId == 99 && body.reason == "NoActiveSession"
            }
        )

        and: "reservationResponse is NOT published"
        0 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-07 — reserveNow: connectorId=0 passed through unchanged (BR-001)
    // ─────────────────────────────────────────────

    void "RS-07 reserveNow passes connectorId=0 to ReserveNowRequest unchanged (BR-001)"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        def capturedRequest = null
        connections.callInline(UUID.fromString(SESSION_ID), _) >> { UUID sid, req ->
            capturedRequest = req
            def conf = new ReserveNowConfirmation()
            conf.status = ReservationStatus.Accepted
            return conf
        }

        def payload = objectMapper.writeValueAsString([
            connectorId  : 0,   // BR-001: connector 0 = whole charge point (OCPP 1.6 §3.11)
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 7
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then: "ReserveNowRequest has connectorId=0 — not modified by SE"
        capturedRequest.connectorId == 0
        1 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-08 — reserveNow: NotConnectedException → no publish (BR-012)
    // ─────────────────────────────────────────────

    void "RS-08 reserveNow on NotConnectedException does not publish anything (BR-012)"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        connections.callInline(UUID.fromString(SESSION_ID), _) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 10
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then: "no Kafka publish — wrong SE instance, another SE instance holds this session"
        0 * incoming.reservationResponse(_, _, _, _)
        0 * incoming.chargerError(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-09 — reserveNow: unexpected exception → ChargerError
    // ─────────────────────────────────────────────

    void "RS-09 reserveNow on unexpected exception publishes ChargerError with ReserveNowError"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        connections.callInline(UUID.fromString(SESSION_ID), _) >> {
            throw new RuntimeException("OCPP internal error")
        }

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 11
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then:
        1 * incoming.chargerError(NODE_ID, IDENTIFIER, "ReserveNowError", _)
        0 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-10 — reserveNow: malformed expiryDate → ChargerError
    // ─────────────────────────────────────────────

    void "RS-10 reserveNow with malformed expiryDate publishes ChargerError with ReserveNowError"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "not-a-valid-date",   // DateTimeParseException expected
            idTag        : "DRIVER-001",
            reservationId: 12
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then:
        1 * incoming.chargerError(NODE_ID, IDENTIFIER, "ReserveNowError", _)
        0 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-11/RS-12 — cancelReservation: both CancelReservationStatus values (BR-009)
    // ─────────────────────────────────────────────

    @Unroll
    void "RS-#testId cancelReservation publishes reservationResponse for status #status (BR-009)"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        def confirmation = new CancelReservationConfirmation()
        confirmation.status = status
        connections.callInline(UUID.fromString(SESSION_ID), _) >> confirmation

        def payload = objectMapper.writeValueAsString([reservationId: 55])

        when:
        reservationService.cancelReservation(IDENTIFIER, payload)

        then: "reservationResponse is published with correct eventType and status"
        1 * incoming.reservationResponse(
            NODE_ID, IDENTIFIER, OCPPEventTypes.CANCEL_RESERVATION_RESPONSE.toString(),
            { String p ->
                def body = objectMapper.readValue(p, Map)
                body.status == status.toString() && body.reservationId == 55
            }
        )
        0 * incoming.chargerError(_, _, _, _)

        where:
        testId | status
        "11"   | CancelReservationStatus.Accepted
        "12"   | CancelReservationStatus.Rejected
    }

    // ─────────────────────────────────────────────
    // RS-13 — cancelReservation: null session → ChargerError (BR-007)
    // ─────────────────────────────────────────────

    void "RS-13 cancelReservation with no active session publishes ChargerError (BR-007, BR-008)"() {
        given:
        connections.getLatestSessionId("EVK-OFFLINE") >> null

        def payload = objectMapper.writeValueAsString([reservationId: 77])

        when:
        reservationService.cancelReservation("EVK-OFFLINE", payload)

        then:
        1 * incoming.chargerError(
            NODE_ID, "EVK-OFFLINE", "CancelReservationError",
            { String p ->
                def body = objectMapper.readValue(p, Map)
                body.reservationId == 77 && body.reason == "NoActiveSession"
            }
        )
        0 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-14 — cancelReservation: NotConnectedException → no publish (BR-012)
    // ─────────────────────────────────────────────

    void "RS-14 cancelReservation on NotConnectedException does not publish anything (BR-012)"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        connections.callInline(UUID.fromString(SESSION_ID), _) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([reservationId: 88])

        when:
        reservationService.cancelReservation(IDENTIFIER, payload)

        then:
        0 * incoming.reservationResponse(_, _, _, _)
        0 * incoming.chargerError(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-15 — cancelReservation: unexpected exception → ChargerError
    // ─────────────────────────────────────────────

    void "RS-15 cancelReservation on unexpected exception publishes ChargerError with CancelReservationError"() {
        given:
        connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        connections.callInline(UUID.fromString(SESSION_ID), _) >> {
            throw new RuntimeException("unexpected charger failure")
        }

        def payload = objectMapper.writeValueAsString([reservationId: 99])

        when:
        reservationService.cancelReservation(IDENTIFIER, payload)

        then:
        1 * incoming.chargerError(NODE_ID, IDENTIFIER, "CancelReservationError", _)
        0 * incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-16 — reserveNow: null confirmation guard → ChargerError
    // ─────────────────────────────────────────────

    void "RS-16 reserveNow with null confirmation from callInline publishes ChargerError with ReserveNowError"() {
        given: "callInline returns null — internal timeout swallowed by eu.chargetime library"
        reservationService.connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        reservationService.connections.callInline(UUID.fromString(SESSION_ID), _) >> null

        def payload = objectMapper.writeValueAsString([
            connectorId  : 1,
            expiryDate   : "2026-04-10T14:30:00Z",
            idTag        : "DRIVER-001",
            reservationId: 20
        ])

        when:
        reservationService.reserveNow(IDENTIFIER, payload)

        then: "chargerError published with NullConfirmation reason"
        1 * reservationService.incoming.chargerError(NODE_ID, IDENTIFIER, "ReserveNowError", _)
        0 * reservationService.incoming.reservationResponse(_, _, _, _)
    }

    // ─────────────────────────────────────────────
    // RS-17 — cancelReservation: null confirmation guard → ChargerError
    // ─────────────────────────────────────────────

    void "RS-17 cancelReservation with null confirmation from callInline publishes ChargerError with CancelReservationError"() {
        given: "callInline returns null — internal timeout swallowed by eu.chargetime library"
        reservationService.connections.getLatestSessionId(IDENTIFIER) >> SESSION_ID
        reservationService.connections.callInline(UUID.fromString(SESSION_ID), _) >> null

        def payload = objectMapper.writeValueAsString([reservationId: 33])

        when:
        reservationService.cancelReservation(IDENTIFIER, payload)

        then: "chargerError published with NullConfirmation reason"
        1 * reservationService.incoming.chargerError(NODE_ID, IDENTIFIER, "CancelReservationError", _)
        0 * reservationService.incoming.reservationResponse(_, _, _, _)
    }
}
