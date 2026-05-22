package se

import com.fasterxml.jackson.databind.ObjectMapper
import se.bus.OcppEventReceiver
import se.service.MtlsHandshakeListener
import spock.lang.Specification
import spock.lang.Subject

import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSession
import java.security.cert.X509Certificate
import javax.security.auth.x500.X500Principal

/**
 * Unit tests for US-CERT106-01: MtlsHandshakeListener
 *
 * Tests CN extraction from client certificate and InvalidChargePointCertificate
 * SecurityEvent publication to Kafka.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3, §5
 * [OCA-CERT-RELEVANT] TC-69, TC-70
 */
class MtlsHandshakeListenerSpec extends Specification {

    @Subject
    MtlsHandshakeListener listener

    OcppEventReceiver incoming = Mock()
    EdgeConfig edgeConfig = Mock()
    ObjectMapper objectMapper = new ObjectMapper()

    def setup() {
        edgeConfig.kafkaNodeId >> "000"
        listener = new MtlsHandshakeListener(incoming, objectMapper, edgeConfig)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-012: Valid CN extracted from client cert
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-012: Valid CN extracted — chargePointId from cert CN"() {
        given: "SSL session with client cert CN=CP001"
        def sslSession = Mock(SSLSession)
        def cert = Mock(X509Certificate)
        cert.getSubjectX500Principal() >> new X500Principal("CN=CP001,O=EVoke,C=US")
        sslSession.getPeerCertificates() >> ([cert] as java.security.cert.Certificate[])

        def cause = new Exception("validity check failed")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("192.168.1.100", cause, sslSession)

        then: "SecurityEvent published with chargePointId=CP001"
        1 * incoming.securityEvent("000", "CP001", "InvalidChargePointCertificate", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP001" &&
            body.type == "InvalidChargePointCertificate" &&
            body.sourceIp == "192.168.1.100" &&
            body.reason.contains("validity") &&
            body.timestamp != null
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-013: CN extraction fails (no cert) — fallback to unknown
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-013: No cert in session — chargePointId = unknown"() {
        given: "Null SSL session"
        def cause = new Exception("Empty client certificate chain")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("10.0.0.55", cause, null)

        then: "SecurityEvent published with chargePointId=unknown"
        1 * incoming.securityEvent("000", "unknown", "InvalidChargePointCertificate", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "unknown" &&
            body.sourceIp == "10.0.0.55"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-014: Malformed DN (no CN) — fallback to unknown
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-014: Malformed DN without CN — chargePointId = unknown"() {
        given: "SSL session with cert that has no CN in DN"
        def sslSession = Mock(SSLSession)
        def cert = Mock(X509Certificate)
        cert.getSubjectX500Principal() >> new X500Principal("O=SomeOrg,C=US")
        sslSession.getPeerCertificates() >> ([cert] as java.security.cert.Certificate[])

        def cause = new Exception("certificate chain error")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("172.16.0.1", cause, sslSession)

        then: "SecurityEvent published with chargePointId=unknown"
        1 * incoming.securityEvent("000", "unknown", "InvalidChargePointCertificate", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "unknown" &&
            body.sourceIp == "172.16.0.1"
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-015: Expired client cert
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-015: Expired client cert — InvalidChargePointCertificate published"() {
        given: "SSL session with expired cert CN=CP-EXPIRED"
        def sslSession = Mock(SSLSession)
        def cert = Mock(X509Certificate)
        cert.getSubjectX500Principal() >> new X500Principal("CN=CP-EXPIRED")
        sslSession.getPeerCertificates() >> ([cert] as java.security.cert.Certificate[])

        def cause = new Exception("PKIX path validation failed: validity check failed")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("192.168.1.200", cause, sslSession)

        then: "SecurityEvent published with correct fields"
        1 * incoming.securityEvent("000", "CP-EXPIRED", "InvalidChargePointCertificate", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP-EXPIRED" &&
            body.type == "InvalidChargePointCertificate" &&
            body.reason.contains("validity")
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-016: Self-signed cert
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-016: Self-signed cert — InvalidChargePointCertificate published"() {
        given: "SSL session with self-signed cert CN=CP-ROGUE"
        def sslSession = Mock(SSLSession)
        def cert = Mock(X509Certificate)
        cert.getSubjectX500Principal() >> new X500Principal("CN=CP-ROGUE")
        sslSession.getPeerCertificates() >> ([cert] as java.security.cert.Certificate[])

        def cause = new Exception("unable to find valid certification path to requested target")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("10.0.0.55", cause, sslSession)

        then: "SecurityEvent published"
        1 * incoming.securityEvent("000", "CP-ROGUE", "InvalidChargePointCertificate", {
            def body = objectMapper.readValue(it as String, Map)
            body.chargePointId == "CP-ROGUE" &&
            body.reason.contains("certification path")
        })
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-017: Kafka publish failure — no exception propagated
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-017: Kafka failure — no exception propagated"() {
        given: "Kafka is unavailable"
        incoming.securityEvent(_, _, _, _) >> { throw new RuntimeException("Kafka unavailable") }

        def sslSession = Mock(SSLSession)
        sslSession.getPeerCertificates() >> { throw new SSLPeerUnverifiedException("no cert") }

        def cause = new Exception("handshake failure")

        when: "Handshake failure is handled"
        listener.handleHandshakeFailure("10.0.0.1", cause, sslSession)

        then: "No exception propagated"
        noExceptionThrown()
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-01-018: Multiple rapid failures — each published independently
    // -----------------------------------------------------------------------
    def "TC-CERT106-01-018: 3 rapid failures — 3 separate SecurityEvents"() {
        given: "3 different failure scenarios"
        def session1 = Mock(SSLSession)
        session1.getPeerCertificates() >> { throw new SSLPeerUnverifiedException("no cert") }
        def session2 = Mock(SSLSession)
        session2.getPeerCertificates() >> { throw new SSLPeerUnverifiedException("no cert") }
        def session3 = Mock(SSLSession)
        session3.getPeerCertificates() >> { throw new SSLPeerUnverifiedException("no cert") }

        when: "3 failures processed in rapid succession"
        listener.handleHandshakeFailure("10.0.0.1", new Exception("fail1"), session1)
        listener.handleHandshakeFailure("10.0.0.2", new Exception("fail2"), session2)
        listener.handleHandshakeFailure("10.0.0.3", new Exception("fail3"), session3)

        then: "3 separate SecurityEvents published"
        3 * incoming.securityEvent("000", "unknown", "InvalidChargePointCertificate", _)
    }

    // -----------------------------------------------------------------------
    // extractCpIdentityFromCert — unit tests
    // -----------------------------------------------------------------------
    def "extractCpIdentityFromCert — null session returns unknown"() {
        expect:
        listener.extractCpIdentityFromCert(null) == "unknown"
    }

    def "extractCpIdentityFromCert — SSLPeerUnverifiedException returns unknown"() {
        given:
        def session = Mock(SSLSession)
        session.getPeerCertificates() >> { throw new SSLPeerUnverifiedException("not verified") }

        expect:
        listener.extractCpIdentityFromCert(session) == "unknown"
    }

    def "extractCpIdentityFromCert — valid CN=CP-TEST-01 extracted"() {
        given:
        def session = Mock(SSLSession)
        def cert = Mock(X509Certificate)
        cert.getSubjectX500Principal() >> new X500Principal("CN=CP-TEST-01,O=Test,C=US")
        session.getPeerCertificates() >> ([cert] as java.security.cert.Certificate[])

        expect:
        listener.extractCpIdentityFromCert(session) == "CP-TEST-01"
    }

    def "extractCpIdentityFromCert — empty cert array returns unknown"() {
        given:
        def session = Mock(SSLSession)
        session.getPeerCertificates() >> ([] as java.security.cert.Certificate[])

        expect:
        listener.extractCpIdentityFromCert(session) == "unknown"
    }
}

