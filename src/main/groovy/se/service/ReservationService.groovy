package se.service

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.reservation.CancelReservationConfirmation
import eu.chargetime.ocpp.model.reservation.CancelReservationRequest
import eu.chargetime.ocpp.model.reservation.ReserveNowConfirmation
import eu.chargetime.ocpp.model.reservation.ReserveNowRequest
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.OCPPEventTypes

import java.time.ZonedDateTime

/**
 * Business logic for OCPP 1.6 Reservation actions.
 * Receives commands from OcppEventBroadcaster, translates to OCPP requests,
 * calls the charger synchronously via ConnectionService, and publishes results
 * via OcppEventReceiver.
 *
 * OCPP 1.6 §3.11 ReserveNow, §3.3 CancelReservation
 */
@Singleton
class ReservationService {

    private static final Logger logger = LoggerFactory.getLogger(ReservationService.class)
    boolean kafkaEnabled = true
    final ObjectMapper objectMapper
    final EdgeConfig edgeConfig
    final ConnectionService connections
    final OcppEventReceiver incoming
    final MeterRegistry meterRegistry
    final MessageProducer messageProducer

    @Inject
    ReservationService(ObjectMapper objectMapper,
                       EdgeConfig edgeConfig,
                       ConnectionService connections,
                       OcppEventReceiver incoming,
                       MeterRegistry meterRegistry,
                       MessageProducer messageProducer) {
        this.objectMapper    = objectMapper
        this.edgeConfig      = edgeConfig
        this.connections     = connections
        this.incoming        = incoming
        this.meterRegistry   = meterRegistry
        this.messageProducer = messageProducer
    }

    /**
     * OCPP 1.6 §3.11 ReserveNow.
     * Translates a Kafka ReserveNow command into a ReserveNow.req sent to the charger
     * over its active WebSocket session, then publishes the charger's response to
     * the ReservationResponse Kafka topic.
     *
     * @param identifier charger identifier (e.g. "EVK-001")
     * @param payload    JSON string: {connectorId, expiryDate (ISO 8601), idTag, reservationId}
     */
    void reserveNow(String identifier, String payload) {
        logger.debug("Processing reserveNow for charger {} eventType {}", identifier, OCPPEventTypes.RESERVE_NOW.toString())

        // Step 2: Parse reservationId early — needed for ChargerError payload (BR-008)
        Map payloadMap = objectMapper.readValue(payload, Map.class)
        Integer reservationId = payloadMap.reservationId as Integer

        // Step 3: Look up active session
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            logger.warn("No active session found for charger {} reserveNow reservationId {}", identifier, reservationId)
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", [identifier: identifier], 1)
            // BR-007: publish ChargerError with reservationId for CSM correlation (BR-008)
            Map errorPayload = [identifier: identifier, reservationId: reservationId, reason: "NoActiveSession"]
            incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "ReserveNowError", objectMapper.writeValueAsString(errorPayload))
            return
        }

        // Step 4-5: Parse remaining fields
        Integer connectorId   = payloadMap.connectorId as Integer       // BR-001: pass through unchanged (0 = whole charger)
        String  expiryDateStr = payloadMap.expiryDate as String         // BR-002: pass through as-is
        String  idTag         = payloadMap.idTag as String              // BR-003: must not truncate (max 20 chars per OCPP 1.6 §3.11)

        ReserveNowConfirmation confirmation = null
        try {
            // ZonedDateTime.parse uses ISO 8601 — same pattern as OcppEventBroadcaster.sendLocalAuthorizationList()
            ZonedDateTime expiryDate = ZonedDateTime.parse(expiryDateStr)

            // Step 6: Build OCPP 1.6 §3.11 ReserveNowRequest
            ReserveNowRequest request = new ReserveNowRequest()
            request.connectorId   = connectorId
            request.expiryDate    = expiryDate
            request.idTag         = idTag
            request.reservationId = reservationId

            // Step 7: Send to charger synchronously
            confirmation = (ReserveNowConfirmation) connections.callInline(UUID.fromString(sessionId), request)

            if (confirmation == null) {
                // callInline returned null — internal timeout swallowed; treat as error
                logger.error("Null confirmation returned for ReserveNow charger {} reservationId {}", identifier, reservationId)
                incoming.chargerError(
                    edgeConfig.kafkaNodeId, identifier, "ReserveNowError",
                    objectMapper.writeValueAsString([identifier: identifier, reservationId: reservationId, reason: "NullConfirmation"])
                )
                return
            }

            // Step 8: Publish for ALL 5 ReservationStatus values (BR-005, BR-006)
            logger.debug("ReserveNow response for charger {} reservationId {} status {}", identifier, reservationId, confirmation.status)
            incoming.reservationResponse(
                edgeConfig.kafkaNodeId,
                identifier,
                OCPPEventTypes.RESERVE_NOW_RESPONSE.toString(),
                objectMapper.writeValueAsString([status: confirmation.status.toString(), reservationId: reservationId])
            )

        } catch (NotConnectedException nce) {
            // BR-012: trace only — wrong SE instance; another SE instance holds this session
            logger.trace("Probable wrong SE instance for charger {} reserveNow", identifier)

        } catch (Exception e) {
            // ERR-3: unexpected exception → ChargerError with reservationId (BR-008)
            logger.error("Error processing ReserveNow for charger {} reservationId {}: {}", identifier, reservationId, e.getMessage(), e)
            incoming.chargerError(
                edgeConfig.kafkaNodeId,
                identifier,
                "ReserveNowError",
                objectMapper.writeValueAsString([identifier: identifier, reservationId: reservationId, reason: e.getMessage()])
            )
        }
    }

    /**
     * OCPP 1.6 §3.3 CancelReservation.
     * Translates a Kafka CancelReservation command into a CancelReservation.req sent to the charger
     * over its active WebSocket session, then publishes the charger's response to
     * the ReservationResponse Kafka topic.
     *
     * @param identifier charger identifier (e.g. "EVK-001")
     * @param payload    JSON string: {reservationId}
     */
    void cancelReservation(String identifier, String payload) {
        logger.debug("Processing cancelReservation for charger {} eventType {}", identifier, OCPPEventTypes.CANCEL_RESERVATION.toString())

        // Parse reservationId early — needed for ChargerError payload (BR-008)
        Map payloadMap = objectMapper.readValue(payload, Map.class)
        Integer reservationId = payloadMap.reservationId as Integer

        // Look up active session
        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId) {
            logger.warn("No active session found for charger {} cancelReservation reservationId {}", identifier, reservationId)
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", [identifier: identifier], 1)
            // BR-007: publish ChargerError with reservationId for CSM correlation (BR-008)
            Map errorPayload = [identifier: identifier, reservationId: reservationId, reason: "NoActiveSession"]
            incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "CancelReservationError", objectMapper.writeValueAsString(errorPayload))
            return
        }

        CancelReservationConfirmation confirmation = null
        try {
            // Build OCPP 1.6 §3.3 CancelReservationRequest
            CancelReservationRequest request = new CancelReservationRequest()
            request.reservationId = reservationId

            // Send to charger synchronously
            confirmation = (CancelReservationConfirmation) connections.callInline(UUID.fromString(sessionId), request)

            if (confirmation == null) {
                // callInline returned null — internal timeout swallowed; treat as error
                logger.error("Null confirmation returned for CancelReservation charger {} reservationId {}", identifier, reservationId)
                incoming.chargerError(
                    edgeConfig.kafkaNodeId, identifier, "CancelReservationError",
                    objectMapper.writeValueAsString([identifier: identifier, reservationId: reservationId, reason: "NullConfirmation"])
                )
                return
            }

            // Publish for both CancelReservationStatus values: Accepted, Rejected (BR-009)
            logger.debug("CancelReservation response for charger {} reservationId {} status {}", identifier, reservationId, confirmation.status)
            incoming.reservationResponse(
                edgeConfig.kafkaNodeId,
                identifier,
                OCPPEventTypes.CANCEL_RESERVATION_RESPONSE.toString(),
                objectMapper.writeValueAsString([status: confirmation.status.toString(), reservationId: reservationId])
            )

        } catch (NotConnectedException nce) {
            // BR-012: trace only — wrong SE instance; another SE instance holds this session
            logger.trace("Probable wrong SE instance for charger {} cancelReservation", identifier)

        } catch (Exception e) {
            // ERR-3: unexpected exception → ChargerError with reservationId (BR-008)
            logger.error("Error processing CancelReservation for charger {} reservationId {}: {}", identifier, reservationId, e.getMessage(), e)
            incoming.chargerError(
                edgeConfig.kafkaNodeId,
                identifier,
                "CancelReservationError",
                objectMapper.writeValueAsString([identifier: identifier, reservationId: reservationId, reason: e.getMessage()])
            )
        }
    }

    private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
        def metricsPayload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
        if(kafkaEnabled)
            messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, metricsPayload)
    }
}
