package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.securityext.DeleteCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.DeleteCertificateRequest
import eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsConfirmation
import eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsRequest
import eu.chargetime.ocpp.model.securityext.InstallCertificateConfirmation
import eu.chargetime.ocpp.model.securityext.InstallCertificateRequest
import eu.chargetime.ocpp.model.securityext.types.CertificateHashDataType
import eu.chargetime.ocpp.model.securityext.types.CertificateStatusEnumType
import eu.chargetime.ocpp.model.securityext.types.CertificateUseEnumType
import eu.chargetime.ocpp.model.securityext.types.DeleteCertificateStatusEnumType
import eu.chargetime.ocpp.model.securityext.types.GetInstalledCertificateStatusEnumType
import eu.chargetime.ocpp.model.securityext.types.HashAlgorithmEnumType
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-CERT105-01: SecurityExt Certificate Management Command Handlers
 *
 * Tests the certificateManagement() Kafka consumer method in OcppEventBroadcaster.
 * Covers InstallCertificate, DeleteCertificate, GetInstalledCertificateIds operations.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §4.1, §4.2, §4.3
 * [OCA-CERT-RELEVANT] TC-57, TC-58, TC-59
 */
class CertificateManagementSpec extends Specification {

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

    // -----------------------------------------------------------------------
    // TC-CERT105-01-01: InstallCertificate ManufacturerRootCertificate — Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-01: InstallCertificate ManufacturerRootCertificate — Accepted"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts the certificate installation"
        def confirmation = new InstallCertificateConfirmation(CertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as InstallCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            certificateType: "ManufacturerRootCertificate",
            certificate: "-----BEGIN CERTIFICATE-----\nMIID...base64...\n-----END CERTIFICATE-----"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then: "Response published to CertificateManagementResponse with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.certificateType == "ManufacturerRootCertificate" &&
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-02: InstallCertificate CentralSystemRootCertificate — Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-02: InstallCertificate CentralSystemRootCertificate — Accepted"() {
        given: "Charge point CP002 is connected"
        def identifier = "CP002"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts the certificate"
        def confirmation = new InstallCertificateConfirmation(CertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as InstallCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            certificateType: "CentralSystemRootCertificate",
            certificate: "-----BEGIN CERTIFICATE-----\nMIID...base64...\n-----END CERTIFICATE-----"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then: "Response published with CentralSystemRootCertificate and Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.certificateType == "CentralSystemRootCertificate" &&
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-03: DeleteCertificate — Accepted
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-03: DeleteCertificate — Accepted"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point accepts the certificate deletion"
        def confirmation = new DeleteCertificateConfirmation(DeleteCertificateStatusEnumType.Accepted)
        connections.callInline(UUID.fromString(sessionId), _ as DeleteCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            hashAlgorithm: "SHA256",
            issuerNameHash: "abc123def456",
            issuerKeyHash: "789ghi012jkl",
            serialNumber: "001"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "DeleteCertificate", payload)

        then: "Response published to CertificateManagementResponse with Accepted"
        1 * incoming.certificateManagementResponse("000", identifier, "DeleteCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Accepted"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-04: GetInstalledCertificateIds — Accepted with certificates
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-04: GetInstalledCertificateIds — Accepted with certificates"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point returns 2 installed certificates"
        def hashData1 = new CertificateHashDataType()
        hashData1.hashAlgorithm = HashAlgorithmEnumType.SHA256
        hashData1.issuerNameHash = "hash1"
        hashData1.issuerKeyHash = "key1"
        hashData1.serialNumber = "001"

        def hashData2 = new CertificateHashDataType()
        hashData2.hashAlgorithm = HashAlgorithmEnumType.SHA256
        hashData2.issuerNameHash = "hash2"
        hashData2.issuerKeyHash = "key2"
        hashData2.serialNumber = "002"

        def confirmation = new GetInstalledCertificateIdsConfirmation(GetInstalledCertificateStatusEnumType.Accepted)
        confirmation.certificateHashData = [hashData1, hashData2] as CertificateHashDataType[]
        connections.callInline(UUID.fromString(sessionId), _ as GetInstalledCertificateIdsRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([certificateType: "CentralSystemRootCertificate"])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "GetInstalledCertificateIds", payload)

        then: "Response published with certificate hash data array"
        1 * incoming.certificateManagementResponse("000", identifier, "GetInstalledCertificateIdsResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Accepted" &&
            body.certificateHashData?.size() == 2
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-05: GetInstalledCertificateIds — NotFound
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-05: GetInstalledCertificateIds — NotFound (no certificates installed)"() {
        given: "Charge point CP003 is connected"
        def identifier = "CP003"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point has no installed certificates"
        def confirmation = new GetInstalledCertificateIdsConfirmation(GetInstalledCertificateStatusEnumType.NotFound)
        connections.callInline(UUID.fromString(sessionId), _ as GetInstalledCertificateIdsRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([certificateType: "ManufacturerRootCertificate"])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "GetInstalledCertificateIds", payload)

        then: "Response published with NotFound and empty certificate list"
        1 * incoming.certificateManagementResponse("000", identifier, "GetInstalledCertificateIdsResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "NotFound" &&
            body.certificateHashData == []
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-06: Charge Point Not Connected — InstallCertificate
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-06: Charge Point Not Connected — InstallCertificate error response published"() {
        given: "Charge point CP-OFFLINE is NOT connected"
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", "{}")

        then: "Error response published with ChargePointNotConnected"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "ChargePointNotConnected" &&
            body.operationType == "InstallCertificate"
        })

        and: "No OCPP request sent"
        0 * connections.callInline(_, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-07: Charge Point Not Connected — DeleteCertificate
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-07: Charge Point Not Connected — DeleteCertificate error response published"() {
        given: "Charge point CP-OFFLINE is NOT connected"
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "DeleteCertificate", "{}")

        then: "Error response published with ChargePointNotConnected"
        1 * incoming.certificateManagementResponse("000", identifier, "DeleteCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "ChargePointNotConnected" &&
            body.operationType == "DeleteCertificate"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-08: Charge Point Not Connected — GetInstalledCertificateIds
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-08: Charge Point Not Connected — GetInstalledCertificateIds error response published"() {
        given: "Charge point CP-OFFLINE is NOT connected"
        def identifier = "CP-OFFLINE"
        connections.getLatestSessionId(identifier) >> null

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "GetInstalledCertificateIds", "{}")

        then: "Error response published with ChargePointNotConnected"
        1 * incoming.certificateManagementResponse("000", identifier, "GetInstalledCertificateIdsResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.status == "ChargePointNotConnected" &&
            body.operationType == "GetInstalledCertificateIds"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-09: InstallCertificate — Rejected by charge point
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-09: InstallCertificate — Rejected by charge point"() {
        given: "Charge point CP004 is connected"
        def identifier = "CP004"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point rejects the certificate"
        def confirmation = new InstallCertificateConfirmation(CertificateStatusEnumType.Rejected)
        connections.callInline(UUID.fromString(sessionId), _ as InstallCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            certificateType: "ManufacturerRootCertificate",
            certificate: "-----BEGIN CERTIFICATE-----\nINVALID\n-----END CERTIFICATE-----"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then: "Response published with Rejected status"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Rejected"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-10: DeleteCertificate — NotFound by charge point
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-10: DeleteCertificate — NotFound by charge point"() {
        given: "Charge point CP005 is connected"
        def identifier = "CP005"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Certificate not found on charge point"
        def confirmation = new DeleteCertificateConfirmation(DeleteCertificateStatusEnumType.NotFound)
        connections.callInline(UUID.fromString(sessionId), _ as DeleteCertificateRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([
            hashAlgorithm: "SHA256",
            issuerNameHash: "nonexistent",
            issuerKeyHash: "nonexistent",
            serialNumber: "999"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "DeleteCertificate", payload)

        then: "Response published with NotFound status"
        1 * incoming.certificateManagementResponse("000", identifier, "DeleteCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "NotFound"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-11: Unknown eventType — discard with ERROR log
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-11: Unknown eventType — discard with ERROR log, no response"() {
        given: "Charge point CP001 is connected"
        def identifier = "CP001"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        when: "CertificateManagement with unknown eventType is processed"
        broadcaster.certificateManagement("000", identifier, "InvalidEventType", "{}")

        then: "No OCPP request sent"
        0 * connections.callInline(_, _)

        and: "No response published"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-12: OCPP timeout — callInline throws exception
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-12: OCPP timeout — error response published"() {
        given: "Charge point CP006 is connected but unresponsive"
        def identifier = "CP006"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws a timeout exception"
        connections.callInline(UUID.fromString(sessionId), _ as InstallCertificateRequest) >> {
            throw new RuntimeException("Timeout waiting for response")
        }

        def payload = objectMapper.writeValueAsString([
            certificateType: "ManufacturerRootCertificate",
            certificate: "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "InstallCertificate", payload)

        then: "Error response published"
        1 * incoming.certificateManagementResponse("000", identifier, "InstallCertificateResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Error" &&
            body.error != null
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-13: NotConnectedException during callInline — silent discard
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-13: NotConnectedException — silent discard, no response"() {
        given: "Charge point CP007 session exists but WebSocket is closed"
        def identifier = "CP007"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "callInline throws NotConnectedException"
        connections.callInline(UUID.fromString(sessionId), _ as DeleteCertificateRequest) >> {
            throw new NotConnectedException()
        }

        def payload = objectMapper.writeValueAsString([
            hashAlgorithm: "SHA256",
            issuerNameHash: "abc",
            issuerKeyHash: "def",
            serialNumber: "001"
        ])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "DeleteCertificate", payload)

        then: "No response published (silent discard for NCE)"
        0 * incoming.certificateManagementResponse(_, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT105-01-14: GetInstalledCertificateIds — Accepted with null certificateHashData
    // -----------------------------------------------------------------------
    def "TC-CERT105-01-14: GetInstalledCertificateIds — null certificateHashData returns empty array"() {
        given: "Charge point CP008 is connected"
        def identifier = "CP008"
        def sessionId = UUID.randomUUID().toString()
        connections.getLatestSessionId(identifier) >> sessionId

        and: "Charge point returns Accepted with null certificateHashData"
        def confirmation = new GetInstalledCertificateIdsConfirmation(GetInstalledCertificateStatusEnumType.Accepted)
        // certificateHashData is null by default
        connections.callInline(UUID.fromString(sessionId), _ as GetInstalledCertificateIdsRequest) >> confirmation

        def payload = objectMapper.writeValueAsString([certificateType: "CentralSystemRootCertificate"])

        when: "CertificateManagement Kafka message is processed"
        broadcaster.certificateManagement("000", identifier, "GetInstalledCertificateIds", payload)

        then: "Response published with empty certificate array"
        1 * incoming.certificateManagementResponse("000", identifier, "GetInstalledCertificateIdsResponse", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == identifier &&
            body.status == "Accepted" &&
            body.certificateHashData == []
        })
    }
}

