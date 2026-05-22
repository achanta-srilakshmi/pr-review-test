package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.securityext.CertificateSignedConfirmation
import eu.chargetime.ocpp.model.securityext.CertificateSignedRequest
import eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageConfirmation
import eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageRequest
import eu.chargetime.ocpp.model.securityext.SignCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.SignCertificateRequest
import eu.chargetime.ocpp.model.securityext.types.CertificateSignedStatusEnumType
import eu.chargetime.ocpp.model.securityext.types.GenericStatusEnumType
import eu.chargetime.ocpp.model.securityext.types.TriggerMessageStatusEnumType
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-CERT106-02: SignCertificate, CertificateSigned & ExtendedTriggerMessage (OCA TC-68)
 *
 * Tests the three message flows of the OCPP 1.6 charge point certificate renewal flow:
 * 1. ExtendedTriggerMessage — CSMS-initiated (OcppEventBroadcaster)
 * 2. CertificateSigned — CSMS-initiated (OcppEventBroadcaster)
 * 3. SignCertificate — CP-initiated (SecurityExtEH)
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3 — Certificate Renewal Flow
 * [OCA-CERT-RELEVANT] TC-68
 */
class SignCertificateFlowSpec extends Specification {

    // ----- OcppEventBroadcaster dependencies -----
    @Subject
    OcppEventBroadcaster broadcaster

    ConnectionService connections = Mock()
    OcppEventReceiver incoming = Mock()
    MeterRegistry meterRegistry = Mock()
    MessageProducer messageProducer = Mock()
    EdgeConfig edgeConfig = Mock()
    ObjectMapper objectMapper = new ObjectMapper()
    Counter mockCounter = Mock()

    // ----- SecurityExtEH dependencies -----
    @Subject
    SecurityExtEH securityExtEH

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

        securityExtEH = new SecurityExtEH(connections, incoming, objectMapper, edgeConfig)
    }

    // =======================================================================
    // 1. ExtendedTriggerMessage — CSMS-initiated (OcppEventBroadcaster)
    // =======================================================================

    // -----------------------------------------------------------------------
    // TC-CERT106-02-001: ExtendedTriggerMessage SignCertificate — Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-001: ExtendedTriggerMessage SignChargePointCertificate — Accepted"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point responds Accepted"
        def confirmation = new ExtendedTriggerMessageConfirmation(TriggerMessageStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as ExtendedTriggerMessageRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP001",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "Response published to CertificateManagementResponse with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.requestedMessage == "SignChargePointCertificate" &&
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-002: ExtendedTriggerMessage SignCertificate — Rejected
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-002: ExtendedTriggerMessage SignChargePointCertificate — Rejected"() {
        given: "Charge point CP002 is connected"
        def identifier = "CP002"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point responds Rejected"
        def confirmation = new ExtendedTriggerMessageConfirmation(TriggerMessageStatusEnumType.Rejected)
        connections.callInline(UUID.fromString(sessionId), _ as ExtendedTriggerMessageRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP002",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "Response published with Rejected status"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.requestedMessage == "SignChargePointCertificate" &&
            body.status == "Rejected"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-003: ExtendedTriggerMessage — NotImplemented
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-003: ExtendedTriggerMessage — NotImplemented (CP does not support SecurityExt)"() {
        given: "Charge point CP003 is connected"
        def identifier = "CP003"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point responds NotImplemented"
        def confirmation = new ExtendedTriggerMessageConfirmation(TriggerMessageStatusEnumType.NotImplemented)
        connections.callInline(UUID.fromString(sessionId), _ as ExtendedTriggerMessageRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP003",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "Response published with NotImplemented status"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "NotImplemented"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-004: ExtendedTriggerMessage — CP Not Connected
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-004: ExtendedTriggerMessage — CP Not Connected publishes error response"() {
        given: "Charge point CP-OFFLINE is NOT connected"
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP-OFFLINE",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "Error response published with ChargePointNotConnected"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "ChargePointNotConnected" &&
            body.operationType == "ExtendedTriggerMessage"
        })

        and: "No OCPP request sent"
        0 * connections.callInline(_, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-005: ExtendedTriggerMessage — NotConnectedException
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-005: ExtendedTriggerMessage — NotConnectedException silent discard"() {
        given: "Charge point CP005 session exists but WS is closed"
        def identifier = "CP005"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws NotConnectedException"
        connections.callInline(UUID.fromString(sessionId), _ as ExtendedTriggerMessageRequest) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP005",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "No response published — silent discard"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-006: ExtendedTriggerMessage — Generic exception
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-006: ExtendedTriggerMessage — Generic exception publishes error response"() {
        given: "Charge point CP006 is connected"
        def identifier = "CP006"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws RuntimeException"
        connections.callInline(UUID.fromString(sessionId), _ as ExtendedTriggerMessageRequest) >> {
            throw new RuntimeException("Timeout")
        }

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP006",
            requestedMessage: "SignChargePointCertificate"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", payload)

        then: "Error response published"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Error" &&
            body.error != null
        })
    }

    // =======================================================================
    // 2. CertificateSigned — CSMS-initiated (OcppEventBroadcaster)
    // =======================================================================

    def static final PEM_CERT_CHAIN = "-----BEGIN CERTIFICATE-----\nMIID...signed-cert...\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nMIID...ca-cert...\n-----END CERTIFICATE-----"

    // -----------------------------------------------------------------------
    // TC-CERT106-02-007: CertificateSigned — Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-007: CertificateSigned — Accepted"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts the signed certificate"
        def confirmation = new CertificateSignedConfirmation(CertificateSignedStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as CertificateSignedRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP001",
            certificateChain: PEM_CERT_CHAIN
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", payload)

        then: "Response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "CertificateSignedResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-008: CertificateSigned — Rejected by CP
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-008: CertificateSigned — Rejected by CP (key mismatch)"() {
        given: "Charge point CP002 is connected"
        def identifier = "CP002"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point rejects the certificate"
        def confirmation = new CertificateSignedConfirmation(CertificateSignedStatusEnumType.Rejected)
        connections.callInline(UUID.fromString(sessionId), _ as CertificateSignedRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP002",
            certificateChain: PEM_CERT_CHAIN
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", payload)

        then: "Response published with Rejected"
        1 * incoming.certificateManagementResponse("000", identifier, "CertificateSignedResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Rejected"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-009: CertificateSigned — CP Not Connected
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-009: CertificateSigned — CP Not Connected publishes error response"() {
        given: "Charge point CP-OFFLINE is NOT connected"
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP-OFFLINE",
            certificateChain: PEM_CERT_CHAIN
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", payload)

        then: "Error response published with ChargePointNotConnected"
        1 * incoming.certificateManagementResponse("000", identifier, "CertificateSignedResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "ChargePointNotConnected" &&
            body.operationType == "CertificateSigned"
        })

        and: "No OCPP request sent"
        0 * connections.callInline(_, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-010: CertificateSigned — NotConnectedException
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-010: CertificateSigned — NotConnectedException silent discard"() {
        given: "Charge point CP010 session exists but WS is closed"
        def identifier = "CP010"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws NotConnectedException"
        connections.callInline(UUID.fromString(sessionId), _ as CertificateSignedRequest) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP010",
            certificateChain: PEM_CERT_CHAIN
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", payload)

        then: "No response published — silent discard"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-011: CertificateSigned — Generic exception
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-011: CertificateSigned — Generic exception publishes error response"() {
        given: "Charge point CP011 is connected"
        def identifier = "CP011"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws RuntimeException"
        connections.callInline(UUID.fromString(sessionId), _ as CertificateSignedRequest) >> {
            throw new RuntimeException("Connection reset")
        }

        def payload = objectMapper.writeValueAsString([
            chargePointId   : "CP011",
            certificateChain: PEM_CERT_CHAIN
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", payload)

        then: "Error response published"
        1 * incoming.certificateManagementResponse("000", identifier, "CertificateSignedResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Error" &&
            body.error != null
        })
    }

    // =======================================================================
    // 3. SignCertificate — CP-initiated (SecurityExtEH)
    // =======================================================================

    def static final VALID_CSR = "-----BEGIN CERTIFICATE REQUEST-----\nMIIC...csr-data...\n-----END CERTIFICATE REQUEST-----"

    // -----------------------------------------------------------------------
    // TC-CERT106-02-012: SignCertificate — Valid CSR accepted and published
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-012: SignCertificate — Valid CSR accepted and published to Kafka"() {
        given: "Charge point CP001 sends SignCertificate with valid PEM CSR"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP001"

        def request = new SignCertificateRequest(VALID_CSR)

        when: "The handler processes the SignCertificate request"
        def result = securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Returns Accepted"
        result != null
        result.status == GenericStatusEnumType.Accepted

        and: "CSR published to Kafka CertificateManagementResponse with eventType SignCertificateRequest"
        1 * incoming.certificateManagementResponse("000", "CP001", "SignCertificateRequest", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.csr == VALID_CSR &&
            body.timestamp != null
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-013: SignCertificate — Null CSR rejected
    // Note: OCPP library rejects null in constructor, so we use Mock to
    //       simulate a request where getCsr() returns null (defensive test).
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-013: SignCertificate — Null CSR rejected, no Kafka publish"() {
        given: "Charge point sends SignCertificate with null CSR"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-NULL"

        def request = Mock(SignCertificateRequest)
        request.getCsr() >> null

        when: "The handler processes the SignCertificate request"
        def result = securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Returns Rejected"
        result != null
        result.status == GenericStatusEnumType.Rejected

        and: "No Kafka publish"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-014: SignCertificate — Empty CSR rejected
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-014: SignCertificate — Empty CSR rejected, no Kafka publish"() {
        given: "Charge point sends SignCertificate with empty CSR"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-EMPTY"

        def request = new SignCertificateRequest("")

        when: "The handler processes the SignCertificate request"
        def result = securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Returns Rejected"
        result != null
        result.status == GenericStatusEnumType.Rejected

        and: "No Kafka publish"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-015: SignCertificate — Malformed CSR (no PEM marker) rejected
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-015: SignCertificate — Malformed CSR rejected, no Kafka publish"() {
        given: "Charge point sends SignCertificate with non-PEM data"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-MALFORMED"

        def request = new SignCertificateRequest("random-data-not-a-csr")

        when: "The handler processes the SignCertificate request"
        def result = securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Returns Rejected"
        result != null
        result.status == GenericStatusEnumType.Rejected

        and: "No Kafka publish"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-016: SignCertificate — Kafka failure still returns Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-016: SignCertificate — Kafka failure still returns Accepted"() {
        given: "Charge point sends valid CSR but Kafka is unavailable"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-KAFKA-FAIL"

        and: "Kafka publish throws exception"
        incoming.certificateManagementResponse(_, _, _, _) >> { throw new RuntimeException("Kafka unavailable") }

        def request = new SignCertificateRequest(VALID_CSR)

        when: "The handler processes the SignCertificate request"
        def result = securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Returns Accepted despite Kafka failure (non-blocking)"
        result != null
        result.status == GenericStatusEnumType.Accepted
        noExceptionThrown()
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-017: SignCertificate — Charger identity resolved from session
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-017: SignCertificate — Charger identity resolved from session"() {
        given: "Session UUID maps to CP-RENEWAL-01 in ConnectionService"
        def sessionId = UUID.randomUUID()
        connections.getChargerId(sessionId.toString()) >> "CP-RENEWAL-01"

        def request = new SignCertificateRequest(VALID_CSR)

        when: "The handler processes the SignCertificate request"
        securityExtEH.handleSignCertificateRequest(sessionId, request)

        then: "Kafka event contains chargePointId = CP-RENEWAL-01"
        1 * incoming.certificateManagementResponse("000", "CP-RENEWAL-01", "SignCertificateRequest", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP-RENEWAL-01"
        })
    }

    // =======================================================================
    // 4. Regression — existing CERT105 flows
    // =======================================================================

    // -----------------------------------------------------------------------
    // TC-CERT106-02-018: InstallCertificate still works
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-018: InstallCertificate regression — still works after CERT106 changes"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts InstallCertificate"
        def confirmation = new eu.chargetime.ocpp.model.securityext.InstallCertificateConfirmation(
            eu.chargetime.ocpp.model.securityext.types.CertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as eu.chargetime.ocpp.model.securityext.InstallCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            certificateType: "ManufacturerRootCertificate",
            certificate    : "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then: "Response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-019: DeleteCertificate still works
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-019: DeleteCertificate regression — still works after CERT106 changes"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts DeleteCertificate"
        def confirmation = new eu.chargetime.ocpp.model.securityext.DeleteCertificateConfirmation(
            eu.chargetime.ocpp.model.securityext.types.DeleteCertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as eu.chargetime.ocpp.model.securityext.DeleteCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            hashAlgorithm : "SHA256",
            issuerNameHash: "abc123",
            issuerKeyHash : "def456",
            serialNumber  : "001"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "DeleteCertificate", payload)

        then: "Response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "DeleteCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-020: GetInstalledCertificateIds still works
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-020: GetInstalledCertificateIds regression — still works after CERT106 changes"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts GetInstalledCertificateIds"
        def confirmation = new eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsConfirmation(
            eu.chargetime.ocpp.model.securityext.types.GetInstalledCertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([certificateType: "CentralSystemRootCertificate"])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "GetInstalledCertificateIds", payload)

        then: "Response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "GetInstalledCertificateIdsResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-02-021: Unknown eventType handled gracefully
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-021: Unknown eventType — no crash, no response"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "CertificateManagement with unknown eventType is processed"
        broadcaster.certificateManagement("000", identifier, "SomeFutureEventType", "{}")

        then: "No OCPP request sent, no response published"
        0 * connections.callInline(_, _)
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // =======================================================================
    // 5. End-to-end TC-68 flow
    // =======================================================================

    // -----------------------------------------------------------------------
    // TC-CERT106-02-022: Full three-step certificate renewal flow
    // -----------------------------------------------------------------------
    def "TC-CERT106-02-022: Full three-step certificate renewal flow"() {
        given: "Charge point CP001 is connected for all steps"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID()
        def sessionIdStr = sessionId.toString()
        connections.getLatestSessionId(identifier) >> sessionIdStr
        connections.getChargerId(sessionId.toString()) >> identifier

        and: "Step 1: CP accepts ExtendedTriggerMessage"
        def triggerConfirmation = new ExtendedTriggerMessageConfirmation(TriggerMessageStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionIdStr), _ as ExtendedTriggerMessageRequest) >> triggerConfirmation

        and: "Step 3: CP accepts CertificateSigned"
        def certSignedConfirmation = new CertificateSignedConfirmation(CertificateSignedStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionIdStr), _ as CertificateSignedRequest) >> certSignedConfirmation

        when: "Step 1: CSMS sends ExtendedTriggerMessage(SignChargePointCertificate)"
        def triggerPayload = objectMapper.writeValueAsString([
            chargePointId   : identifier,
            requestedMessage: "SignChargePointCertificate"
        ])
        broadcaster.certificateManagement("000", identifier, "ExtendedTriggerMessage", triggerPayload)

        then: "ExtendedTriggerMessage response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "ExtendedTriggerMessageResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted" && body.requestedMessage == "SignChargePointCertificate"
        })

        when: "Step 2: CP sends SignCertificate.req with valid CSR"
        def signRequest = new SignCertificateRequest(VALID_CSR)
        def signResult = securityExtEH.handleSignCertificateRequest(sessionId, signRequest)

        then: "Returns Accepted and CSR published to Kafka"
        signResult.status == GenericStatusEnumType.Accepted
        1 * incoming.certificateManagementResponse("000", identifier, "SignCertificateRequest", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier && body.csr == VALID_CSR
        })

        when: "Step 3: CSMS sends CertificateSigned with PEM chain"
        def certPayload = objectMapper.writeValueAsString([
            chargePointId   : identifier,
            certificateChain: PEM_CERT_CHAIN
        ])
        broadcaster.certificateManagement("000", identifier, "CertificateSigned", certPayload)

        then: "CertificateSigned response published with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "CertificateSignedResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "Accepted" && body.chargePointId == identifier
        })
    }
}

