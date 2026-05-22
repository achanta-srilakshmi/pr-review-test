package se.bus

import se.service.ReservationService
import spock.lang.Specification

/**
 * Pure unit tests for OcppEventBroadcaster's reservation listener delegation.
 * No Micronaut context — OcppEventBroadcaster instantiated directly.
 *
 * These tests verify that the @Topic("ReserveNow") and @Topic("CancelReservation")
 * listener methods correctly delegate to ReservationService without any transformation.
 *
 * Stage 5 QA — T8 requirement: broadcaster delegation tests.
 */
class OcppEventBroadcasterReservationSpec extends Specification {

    ReservationService mockReservationService = Mock(ReservationService)
    OcppEventBroadcaster broadcaster

    def setup() {
        broadcaster = new OcppEventBroadcaster()
        broadcaster.reservationService = mockReservationService
    }

    // ─────────────────────────────────────────────
    // EB-RES-01 — reserveNow delegates to ReservationService (T8)
    // ─────────────────────────────────────────────

    void "EB-RES-01 reserveNow delegates to ReservationService.reserveNow with identifier and payload unchanged (T8)"() {
        given:
        String identifier = "EVK-DELEGATE-001"
        String payload = '{"connectorId":1,"expiryDate":"2026-04-10T14:30:00Z","idTag":"DRIVER-001","reservationId":42}'

        when: "the @Topic('ReserveNow') listener method is invoked"
        broadcaster.reserveNow("bucket-key", identifier, "ReserveNow", payload)

        then: "ReservationService.reserveNow is called with the same identifier and payload — no transformation"
        1 * mockReservationService.reserveNow(identifier, payload)
        0 * mockReservationService.cancelReservation(_, _)
    }

    // ─────────────────────────────────────────────
    // EB-RES-02 — cancelReservation delegates to ReservationService (T8)
    // ─────────────────────────────────────────────

    void "EB-RES-02 cancelReservation delegates to ReservationService.cancelReservation with identifier and payload unchanged (T8)"() {
        given:
        String identifier = "EVK-DELEGATE-002"
        String payload = '{"reservationId":99}'

        when: "the @Topic('CancelReservation') listener method is invoked"
        broadcaster.cancelReservation("bucket-key", identifier, "CancelReservation", payload)

        then: "ReservationService.cancelReservation is called with the same identifier and payload — no transformation"
        1 * mockReservationService.cancelReservation(identifier, payload)
        0 * mockReservationService.reserveNow(_, _)
    }
}
