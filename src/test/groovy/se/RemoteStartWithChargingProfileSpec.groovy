package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.core.ChargingProfile
import eu.chargetime.ocpp.model.core.ChargingProfilePurposeType
import eu.chargetime.ocpp.model.core.ChargingRateUnitType
import eu.chargetime.ocpp.model.core.RecurrencyKindType
import eu.chargetime.ocpp.model.core.RemoteStartStopStatus
import eu.chargetime.ocpp.model.core.RemoteStartTransactionConfirmation
import eu.chargetime.ocpp.model.core.RemoteStartTransactionRequest
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.ocpp16.OCPPEventTypes
import se.service.ChargingProfileService
import se.service.ChargingProfileRequest
import se.service.ChargingProfilePeriod
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-RCP102-01: Remote Start Transaction with Charging Profile
 *
 * Tests validation logic, chargingProfile attachment, and backward compatibility
 * in OcppEventBroadcaster.remoteTransaction() for RemoteStartTransaction with TxProfile.
 *
 * OCPP Compliance: §7.4.15, §3.13, Appendix 2
 */
class RemoteStartWithChargingProfileSpec extends Specification {

    @Subject
    OcppEventBroadcaster broadcaster

    ConnectionService connections = Mock()
    OcppEventReceiver incoming = Mock()
    MeterRegistry meterRegistry = Mock()
    MessageProducer messageProducer = Mock()
    EdgeConfig edgeConfig = Mock()
    ChargingProfileService chargingProfileService = Mock()
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
        broadcaster.chargingProfileService = chargingProfileService
    }

    // ==========================================
    // Step 1: ChargingProfileService.buildChargingProfile() delegator
    // TC-RCP102-01-021
    // ==========================================

    def "TC-021: buildChargingProfile delegates to prepareChargingProfile and returns ChargingProfile"() {
        given: "A ChargingProfileService instance and a valid ChargingProfileRequest"
        def realService = new ChargingProfileService()
        realService.objectMapper = objectMapper

        def request = new ChargingProfileRequest(
            chargingProfileId: 100,
            stackLevel: 1,
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            recurrencyKind: RecurrencyKindType.Daily,
            charging_rate_unit: ChargingRateUnitType.W,
            duration: 3600,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "buildChargingProfile is called"
        def result = realService.buildChargingProfile(request)

        then: "It returns a valid ChargingProfile OCPP object"
        result != null
        result instanceof ChargingProfile
        result.chargingProfileId == 100
        result.stackLevel == 1
        result.chargingProfilePurpose == ChargingProfilePurposeType.TxProfile
        result.chargingSchedule != null
        result.chargingSchedule.chargingSchedulePeriod.length == 1
    }

    // ==========================================
    // Step 2: Validation tests — validateTxProfile()
    // TC-RCP102-01-001 to TC-RCP102-01-009
    // ==========================================

    def "TC-001: valid TxProfile passes validation"() {
        given: "A valid ChargingProfileRequest"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: 1,
            charging_rate_unit: ChargingRateUnitType.W,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns null (no error)"
        result == null
    }

    def "TC-002: invalid purpose TxDefaultProfile is rejected"() {
        given: "A ChargingProfileRequest with TxDefaultProfile"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxDefaultProfile,
            stackLevel: 1,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'must be TxProfile'"
        result != null
        result.contains("must be TxProfile")
    }

    def "TC-003: invalid purpose ChargePointMaxProfile is rejected"() {
        given: "A ChargingProfileRequest with ChargePointMaxProfile"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.ChargePointMaxProfile,
            stackLevel: 1,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'must be TxProfile'"
        result != null
        result.contains("must be TxProfile")
    }

    def "TC-004: connectorId 0 with TxProfile is rejected"() {
        given: "A valid TxProfile request"
        def profile = validTxProfileRequest()

        when: "validateTxProfile is called with connectorId = 0"
        def result = broadcaster.validateTxProfile(profile, 0)

        then: "Returns error containing 'connectorId must be > 0'"
        result != null
        result.contains("connectorId must be > 0")
    }

    def "TC-005: connectorId null with TxProfile is rejected"() {
        given: "A valid TxProfile request"
        def profile = validTxProfileRequest()

        when: "validateTxProfile is called with connectorId = null"
        def result = broadcaster.validateTxProfile(profile, null)

        then: "Returns error containing 'connectorId must be > 0'"
        result != null
        result.contains("connectorId must be > 0")
    }

    def "TC-006: stackLevel 0 is accepted (OCPP 1.6 spec allows >= 0)"() {
        given: "A ChargingProfileRequest with stackLevel = 0"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: 0,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns null (validation passes)"
        result == null
    }

    def "TC-006a: stackLevel negative is rejected"() {
        given: "A ChargingProfileRequest with stackLevel = -1"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: -1,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'stackLevel must be >= 0'"
        result != null
        result.contains("stackLevel must be >= 0")
    }

    def "TC-007: stackLevel null is rejected"() {
        given: "A ChargingProfileRequest with stackLevel = null"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: null,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'stackLevel must be >= 0'"
        result != null
        result.contains("stackLevel must be >= 0")
    }

    def "TC-008: empty charging_profile_period is rejected"() {
        given: "A ChargingProfileRequest with empty periods"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: 1,
            charging_profile_period: []
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'charging_profile_period'"
        result != null
        result.contains("charging_profile_period")
    }

    def "TC-009: null charging_profile_period is rejected"() {
        given: "A ChargingProfileRequest with null periods"
        def profile = new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: 1,
            charging_profile_period: null
        )

        when: "validateTxProfile is called"
        def result = broadcaster.validateTxProfile(profile, 1)

        then: "Returns error containing 'charging_profile_period'"
        result != null
        result.contains("charging_profile_period")
    }

    // ==========================================
    // Step 3: remoteTransaction() with ChargingProfile
    // TC-RCP102-01-010 to TC-RCP102-01-020
    // ==========================================

    def "TC-010: happy path — remote start with valid TxProfile sends chargingProfile on request"() {
        given: "A connected charge point"
        def identifier = "CHARGER-001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier

        and: "Charge point accepts the request"
        RemoteStartTransactionRequest capturedRequest = null
        connections.callInline(_, _) >> { args ->
            capturedRequest = args[1] as RemoteStartTransactionRequest
            return new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Accepted)
        }

        and: "ChargingProfileService returns a valid ChargingProfile"
        def mockProfile = new ChargingProfile()
        mockProfile.chargingProfileId = 100

        when: "RemoteTransaction Kafka message is processed with chargingProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-001",
            chargeSessionId: "session-001",
            chargingProfile: [
                chargingProfileId: 100,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-001", payload)

        then: "buildChargingProfile was called and chargingProfile set on request"
        1 * chargingProfileService.buildChargingProfile(_) >> mockProfile

        and: "RemoteStartTransactionRequest sent with chargingProfile set"
        capturedRequest != null
        capturedRequest.chargingProfile != null
        capturedRequest.chargingProfile.chargingProfileId == 100
        capturedRequest.connectorId == 1
        capturedRequest.idTag == "TAG-001"
    }

    def "TC-011: backward compat — remote start WITHOUT chargingProfile works unchanged"() {
        given: "A connected charge point"
        def identifier = "CHARGER-002"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier

        and: "Charge point accepts the request"
        RemoteStartTransactionRequest capturedRequest = null
        connections.callInline(_, _) >> { args ->
            capturedRequest = args[1] as RemoteStartTransactionRequest
            return new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Accepted)
        }

        when: "RemoteTransaction Kafka message is processed WITHOUT chargingProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-002",
            chargeSessionId: "session-002"
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-002", payload)

        then: "RemoteStartTransactionRequest sent without chargingProfile"
        capturedRequest != null
        capturedRequest.chargingProfile == null
        capturedRequest.connectorId == 1
        capturedRequest.idTag == "TAG-002"

        and: "buildChargingProfile was NOT called"
        0 * chargingProfileService.buildChargingProfile(_)
    }

    def "TC-012: validation failure — invalid purpose rejects before OCPP send"() {
        given: "A connected charge point"
        def identifier = "CHARGER-003"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "RemoteTransaction called with TxDefaultProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-003",
            chargingProfile: [
                chargingProfileId: 200,
                stackLevel: 1,
                chargingProfilePurposeType: "TxDefaultProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-003", payload)

        then: "No OCPP message sent"
        0 * connections.callInline(_, _)

        and: "ChargerError published"
        1 * incoming.chargerError("000", identifier, "RemoteStartTransactionError", _)

        and: "buildChargingProfile NOT called"
        0 * chargingProfileService.buildChargingProfile(_)
    }

    def "TC-013: validation failure — connectorId 0 with TxProfile rejects"() {
        given: "A connected charge point"
        def identifier = "CHARGER-004"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "RemoteTransaction called with connectorIndex = 0"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 0,
            idTag: "TAG-004",
            chargingProfile: [
                chargingProfileId: 300,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-004", payload)

        then: "No OCPP message sent"
        0 * connections.callInline(_, _)

        and: "ChargerError published"
        1 * incoming.chargerError("000", identifier, "RemoteStartTransactionError", { it.contains("connectorId") })
    }

    def "TC-014: validation failure — stackLevel 0 rejects"() {
        given: "A connected charge point"
        def identifier = "CHARGER-005"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "RemoteTransaction called with stackLevel = 0"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-005",
            chargingProfile: [
                chargingProfileId: 400,
                stackLevel: 0,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-005", payload)

        then: "No OCPP message sent"
        0 * connections.callInline(_, _)

        and: "ChargerError published"
        1 * incoming.chargerError("000", identifier, "RemoteStartTransactionError", { it.contains("stackLevel") })
    }

    def "TC-015: validation failure — empty charging_profile_period rejects"() {
        given: "A connected charge point"
        def identifier = "CHARGER-006"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "RemoteTransaction called with empty charging_profile_period"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-006",
            chargingProfile: [
                chargingProfileId: 500,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: []
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-006", payload)

        then: "No OCPP message sent"
        0 * connections.callInline(_, _)

        and: "ChargerError published"
        1 * incoming.chargerError("000", identifier, "RemoteStartTransactionError", { it.contains("charging_profile_period") })
    }

    def "TC-016: charge point rejects profile-bearing request"() {
        given: "A connected charge point that rejects"
        def identifier = "CHARGER-007"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier
        connections.callInline(_, _) >> new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Rejected)

        and: "ChargingProfileService returns a valid ChargingProfile"
        chargingProfileService.buildChargingProfile(_) >> new ChargingProfile()

        when: "RemoteTransaction called with valid chargingProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-007",
            chargeSessionId: "session-007",
            chargingProfile: [
                chargingProfileId: 600,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-007", payload)

        then: "ChargerError published for rejection"
        1 * incoming.chargerError("000", identifier, "RemoteStartTransactionError", _)
    }

    def "TC-017: transactionId is stripped from chargingProfile before building"() {
        given: "A connected charge point"
        def identifier = "CHARGER-008"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier
        connections.callInline(_, _) >> new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Accepted)

        and: "Capture the ChargingProfileRequest passed to buildChargingProfile"
        ChargingProfileRequest capturedProfileRequest = null
        chargingProfileService.buildChargingProfile(_) >> { args ->
            capturedProfileRequest = args[0] as ChargingProfileRequest
            return new ChargingProfile()
        }

        when: "RemoteTransaction called with transactionId in chargingProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-008",
            chargeSessionId: "session-008",
            chargingProfile: [
                chargingProfileId: 700,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                transactionId: 999999,
                charging_profile_period: [[start_period: 0, limit: 7400.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-008", payload)

        then: "transactionId was stripped (set to null) before building"
        capturedProfileRequest != null
        capturedProfileRequest.transactionId == null
    }

    def "TC-018: charger not connected — with chargingProfile discards silently"() {
        given: "A charger that is NOT connected"
        def identifier = "CHARGER-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when: "RemoteTransaction called with chargingProfile"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-OFFLINE",
            chargingProfile: [
                chargingProfileId: 800,
                stackLevel: 1,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "W",
                charging_profile_period: [[start_period: 0, limit: 11000.0]]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-OFF", payload)

        then: "No OCPP message, no profile build"
        0 * connections.callInline(_, _)
        0 * chargingProfileService.buildChargingProfile(_)

        and: "Metric recorded"
        1 * meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier) >> mockCounter
        1 * mockCounter.increment()
    }

    def "TC-019: chargingProfile key present but null value — backward compatible"() {
        given: "A connected charge point"
        def identifier = "CHARGER-009"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier
        connections.callInline(_, _) >> new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Accepted)

        when: "RemoteTransaction called with chargingProfile = null in payload"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-009",
            chargeSessionId: "session-009",
            chargingProfile: null
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-009", payload)

        then: "buildChargingProfile NOT called"
        0 * chargingProfileService.buildChargingProfile(_)
    }

    def "TC-020: multiple charging schedule periods handled correctly"() {
        given: "A connected charge point"
        def identifier = "CHARGER-010"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId
        connections.getChargerId(sessionId) >> identifier
        connections.callInline(_, _) >> new RemoteStartTransactionConfirmation(RemoteStartStopStatus.Accepted)

        and: "Capture the ChargingProfileRequest"
        ChargingProfileRequest capturedProfileRequest = null
        chargingProfileService.buildChargingProfile(_) >> { args ->
            capturedProfileRequest = args[0] as ChargingProfileRequest
            return new ChargingProfile()
        }

        when: "RemoteTransaction called with 3 periods"
        def payload = objectMapper.writeValueAsString([
            connectorIndex: 1,
            idTag: "TAG-010",
            chargeSessionId: "session-010",
            chargingProfile: [
                chargingProfileId: 900,
                stackLevel: 2,
                chargingProfilePurposeType: "TxProfile",
                recurrencyKind: "Daily",
                charging_rate_unit: "A",
                duration: 7200,
                charging_profile_period: [
                    [start_period: 0, limit: 32.0],
                    [start_period: 1800, limit: 16.0],
                    [start_period: 3600, limit: 8.0]
                ]
            ]
        ])
        broadcaster.remoteTransaction("000", identifier, OCPPEventTypes.REMOTE_START_TRANSACTION.toString(), "QR-010", payload)

        then: "buildChargingProfile called with 3-period request"
        capturedProfileRequest != null
        capturedProfileRequest.charging_profile_period.size() == 3
        capturedProfileRequest.stackLevel == 2
    }

    // ==========================================
    // Helper Methods
    // ==========================================

    private ChargingProfileRequest validTxProfileRequest() {
        return new ChargingProfileRequest(
            chargingProfilePurposeType: ChargingProfilePurposeType.TxProfile,
            stackLevel: 1,
            charging_rate_unit: ChargingRateUnitType.W,
            charging_profile_period: [new ChargingProfilePeriod(start_period: 0, limit: 11000.0)]
        )
    }
}

