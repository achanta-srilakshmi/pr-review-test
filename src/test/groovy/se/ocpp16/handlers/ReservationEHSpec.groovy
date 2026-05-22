package se.ocpp16.handlers

import eu.chargetime.ocpp.model.reservation.CancelReservationConfirmation
import eu.chargetime.ocpp.model.reservation.CancelReservationRequest
import eu.chargetime.ocpp.model.reservation.ReserveNowConfirmation
import eu.chargetime.ocpp.model.reservation.ReserveNowRequest
import spock.lang.Specification

/**
 * Unit tests for ReservationEH — the OCPP 1.6 reservation event handler stub.
 *
 * In OCPP 1.6, chargers never initiate ReserveNow or CancelReservation;
 * both are CSMS-initiated. Both handler methods are defensive stubs only.
 *
 * OCPP 1.6 §3.11 (ReserveNow), §3.3 (CancelReservation)
 * Stage 5 QA — covers gap: ReservationEH stub tests missing from Stage 4 report.
 */
class ReservationEHSpec extends Specification {

    ReservationEH handler = new ReservationEH()

    // ─────────────────────────────────────────────
    // REH-01 — handleReserveNowRequest: non-null return (OCPP 1.6 §3.11)
    // ─────────────────────────────────────────────

    void "REH-01 handleReserveNowRequest returns non-null ReserveNowConfirmation (defensive stub)"() {
        given: "a session index and an unexpected charger-originated ReserveNow request"
        UUID sessionIndex = UUID.randomUUID()
        ReserveNowRequest request = new ReserveNowRequest()

        when:
        ReserveNowConfirmation result = handler.handleReserveNowRequest(sessionIndex, request)

        then: "returns non-null — null would cause NPE in eu.chargetime serialiser"
        result != null
        result instanceof ReserveNowConfirmation
    }

    // ─────────────────────────────────────────────
    // REH-02 — handleCancelReservationRequest: non-null return (OCPP 1.6 §3.3)
    // ─────────────────────────────────────────────

    void "REH-02 handleCancelReservationRequest returns non-null CancelReservationConfirmation (defensive stub)"() {
        given: "a session index and an unexpected charger-originated CancelReservation request"
        UUID sessionIndex = UUID.randomUUID()
        CancelReservationRequest request = new CancelReservationRequest()

        when:
        CancelReservationConfirmation result = handler.handleCancelReservationRequest(sessionIndex, request)

        then: "returns non-null — null would cause NPE in eu.chargetime serialiser"
        result != null
        result instanceof CancelReservationConfirmation
    }
}
