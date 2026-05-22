package se.ocpp16.handlers

import eu.chargetime.ocpp.model.reservation.CancelReservationConfirmation
import eu.chargetime.ocpp.model.reservation.CancelReservationRequest
import eu.chargetime.ocpp.model.reservation.ReserveNowConfirmation
import eu.chargetime.ocpp.model.reservation.ReserveNowRequest
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Reservation handler singleton wired into CoordinatingFactory and SoloFactory.
 * eu.chargetime.ocpp v1_6:1.1.0 ServerReservationProfile has a no-arg constructor
 * — there is no ServerReservationEventHandler interface in this library version.
 * Outbound ReserveNow / CancelReservation commands are sent via ReservationService.
 *
 * OCPP 1.6 §3.11 (ReserveNow), §3.3 (CancelReservation)
 */
@Singleton
class ReservationEH {

    private static final Logger logger = LoggerFactory.getLogger(ReservationEH.class)

    /**
     * OCPP 1.6 §3.11. Chargers do not initiate ReserveNow — this is CSMS-initiated only.
     * Defensive stub: returns a non-null confirmation to prevent NPE in the eu.chargetime serialiser.
     */
    ReserveNowConfirmation handleReserveNowRequest(UUID sessionIndex, ReserveNowRequest request) {
        logger.warn("Unexpected charger-originated ReserveNow received from session {} — ignoring", sessionIndex)
        return new ReserveNowConfirmation()
    }

    /**
     * OCPP 1.6 §3.3. Chargers do not initiate CancelReservation — this is CSMS-initiated only.
     * Defensive stub: returns a non-null confirmation to prevent NPE in the eu.chargetime serialiser.
     */
    CancelReservationConfirmation handleCancelReservationRequest(UUID sessionIndex, CancelReservationRequest request) {
        logger.warn("Unexpected charger-originated CancelReservation received from session {} — ignoring", sessionIndex)
        return new CancelReservationConfirmation()
    }
}
