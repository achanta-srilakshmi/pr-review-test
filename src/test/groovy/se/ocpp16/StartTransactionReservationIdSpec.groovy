package se.ocpp16

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.core.StartTransactionRequest
import spock.lang.Specification

/**
 * Regression tests for Story 3 — StartTransaction links to a fulfilled reservation.
 *
 * BA document finding: "No implementation work required in SE."
 * The existing CoreProfile16EH.handleStartTransactionRequest() uses:
 *   request.properties.each { evokePayload.put(it.key, it.value) }
 * This automatically serialises ALL StartTransactionRequest bean properties
 * into the OCPPTransaction Kafka payload, including the optional reservationId field.
 *
 * These tests verify the library contract assumption the BA relied on — that Groovy
 * bean introspection of StartTransactionRequest exposes the reservationId property.
 *
 * OCPP 1.6 §3.6 (StartTransaction)
 * Business Rules: BR-010, BR-011
 * Stage 5 QA — covers gap: Story 3 regression test explicitly required by BA document.
 */
class StartTransactionReservationIdSpec extends Specification {

    ObjectMapper objectMapper = new ObjectMapper()

    // ─────────────────────────────────────────────
    // ST-RES-01 — reservationId present in OCPPTransaction payload (BR-010)
    // ─────────────────────────────────────────────

    void "ST-RES-01 StartTransactionRequest.properties includes reservationId when set (BR-010, OCPP 1.6 §3.6)"() {
        given: "a StartTransactionRequest with reservationId set (charger fulfilled a reservation)"
        StartTransactionRequest request = new StartTransactionRequest()
        request.connectorId = 1
        request.idTag = "DRIVER-TAG-001"
        request.meterStart = 0
        request.reservationId = 42         // OCPP 1.6 §3.6 optional field

        when: "CoreProfile16EH serialises request via Groovy bean introspection (same as production code)"
        def evokePayload = [:]
        request.properties.each { evokePayload.put(it.key, it.value) }
        String json = objectMapper.writeValueAsString(evokePayload)
        Map parsedPayload = objectMapper.readValue(json, Map)

        then: "reservationId is present in the OCPPTransaction Kafka payload"
        parsedPayload.containsKey("reservationId")
        parsedPayload.reservationId == 42
    }

    // ─────────────────────────────────────────────
    // ST-RES-02 — reservationId is null when not sent by charger (BR-011)
    // ─────────────────────────────────────────────

    void "ST-RES-02 StartTransactionRequest.properties has null reservationId when not set by charger (BR-011, OCPP 1.6 §3.6)"() {
        given: "a StartTransactionRequest without reservationId (normal walk-up charge, no reservation)"
        StartTransactionRequest request = new StartTransactionRequest()
        request.connectorId = 1
        request.idTag = "DRIVER-TAG-002"
        request.meterStart = 0
        // reservationId intentionally not set

        when: "CoreProfile16EH serialises request via Groovy bean introspection (same as production code)"
        def evokePayload = [:]
        request.properties.each { evokePayload.put(it.key, it.value) }
        String json = objectMapper.writeValueAsString(evokePayload)
        Map parsedPayload = objectMapper.readValue(json, Map)

        then: "reservationId appears as null in the payload (CSM must handle null gracefully per BR-011)"
        parsedPayload.containsKey("reservationId")
        parsedPayload.reservationId == null
    }
}
