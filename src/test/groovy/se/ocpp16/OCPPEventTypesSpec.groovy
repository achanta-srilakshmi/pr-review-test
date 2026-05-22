package se.ocpp16

import spock.lang.Specification

/**
 * Regression tests for OCPPEventTypes enum constants added for the Reservations feature.
 * Covers Task T1 acceptance criteria from Stage 3 Architect manifest.
 *
 * OCPP 1.6 §3.11 (ReserveNow), §3.3 (CancelReservation)
 * Stage 5 QA — covers gap: T1 acceptance criteria not asserted in Stage 4 tests.
 */
class OCPPEventTypesSpec extends Specification {

    // ─────────────────────────────────────────────
    // ENUM-01 to ENUM-04 — New reservation enum values (T1 acceptance criteria)
    // ─────────────────────────────────────────────

    void "ENUM-01 RESERVE_NOW toString returns 'ReserveNow' (OCPP 1.6 §3.11)"() {
        expect:
        OCPPEventTypes.RESERVE_NOW.toString() == "ReserveNow"
    }

    void "ENUM-02 RESERVE_NOW_RESPONSE toString returns 'ReserveNow' (same eventType string per ReservationResponse BA contract)"() {
        expect:
        OCPPEventTypes.RESERVE_NOW_RESPONSE.toString() == "ReserveNow"
    }

    void "ENUM-03 CANCEL_RESERVATION toString returns 'CancelReservation' (OCPP 1.6 §3.3)"() {
        expect:
        OCPPEventTypes.CANCEL_RESERVATION.toString() == "CancelReservation"
    }

    void "ENUM-04 CANCEL_RESERVATION_RESPONSE toString returns 'CancelReservation' (same eventType string per ReservationResponse BA contract)"() {
        expect:
        OCPPEventTypes.CANCEL_RESERVATION_RESPONSE.toString() == "CancelReservation"
    }

    // ─────────────────────────────────────────────
    // ENUM-05 — Total count: 23 entries (19 original + 4 reservation)
    // ─────────────────────────────────────────────

    void "ENUM-05 OCPPEventTypes has exactly 23 entries — 19 original plus 4 reservation entries (T1 AC)"() {
        expect:
        OCPPEventTypes.values().length == 23
    }

    // ─────────────────────────────────────────────
    // ENUM-06 — Regression: all 19 original entries unchanged (Do Not Break)
    // ─────────────────────────────────────────────

    void "ENUM-06 all 19 original OCPPEventTypes entries remain unchanged (regression — Do Not Break)"() {
        expect:
        OCPPEventTypes.NEW_SESSION.toString()                        == "NewSession"
        OCPPEventTypes.INACTIVE_CHARGER.toString()                   == "InactiveCharger"
        OCPPEventTypes.LOST_SESSION.toString()                       == "LostSession"
        OCPPEventTypes.BOOT_NOTIFICATION.toString()                  == "BootNotification"
        OCPPEventTypes.METER_VALUES.toString()                       == "MeterValues"
        OCPPEventTypes.STATUS_NOTIFICATION.toString()                == "StatusNotification"
        OCPPEventTypes.START_TRANSACTION.toString()                  == "StartTransaction"
        OCPPEventTypes.STOP_TRANSACTION.toString()                   == "StopTransaction"
        OCPPEventTypes.REMOTE_START_TRANSACTION.toString()           == "RemoteStartTransaction"
        OCPPEventTypes.REMOTE_START_TRANSACTION_RESPONSE.toString()  == "RemoteStartTransactionResponse"
        OCPPEventTypes.REMOTE_STOP_TRANSACTION.toString()            == "RemoteStopTransaction"
        OCPPEventTypes.REMOTE_STOP_TRANSACTION_RESPONSE.toString()   == "RemoteStopTransactionResponse"
        OCPPEventTypes.SET_CHARGING_PROFILE.toString()               == "SetChargingProfile"
        OCPPEventTypes.SET_CHARGING_PROFILE_RESPONSE.toString()      == "SetChargingProfileResponse"
        OCPPEventTypes.GET_CHARGING_PROFILE.toString()               == "GetChargingProfile"
        OCPPEventTypes.GET_CHARGING_PROFILE_RESPONSE.toString()      == "GetChargingProfileResponse"
        OCPPEventTypes.CLEAR_CHARGING_PROFILE.toString()             == "ClearChargingProfile"
        OCPPEventTypes.CLEAR_CHARGING_PROFILE_RESPONSE.toString()    == "ClearChargingProfileResponse"
        OCPPEventTypes.STATION_AUTH.toString()                       == "StationAuth"
    }
}
