package se.service

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.OcppEventReceiver

import javax.naming.ldap.LdapName
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSession
import java.security.cert.X509Certificate
import java.time.Instant

/**
 * Handles mTLS handshake failures by extracting charge point identity from the
 * client certificate CN field and publishing InvalidChargePointCertificate
 * security events to Kafka.
 *
 * This is a CSMS-generated event (not CP-generated like InvalidCentralSystemCertificate).
 * Published to the same SecurityEvent Kafka topic with a different eventType header.
 *
 * US-CERT106-01: mTLS Infrastructure (TC-70, TC-69)
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3, §5
 */
@Singleton
class MtlsHandshakeListener {

    private static final Logger logger = LoggerFactory.getLogger(MtlsHandshakeListener)

    private final OcppEventReceiver incoming
    private final ObjectMapper objectMapper
    private final EdgeConfig edgeConfig

    @Inject
    MtlsHandshakeListener(OcppEventReceiver incoming, ObjectMapper objectMapper, EdgeConfig edgeConfig) {
        this.incoming = incoming
        this.objectMapper = objectMapper
        this.edgeConfig = edgeConfig
    }

    /**
     * Called when a TLS handshake fails due to an invalid client certificate.
     * Extracts charge point identity from the client cert CN, falls back to "unknown",
     * and publishes an InvalidChargePointCertificate SecurityEvent to Kafka.
     *
     * Non-blocking — Kafka publication failure does not propagate.
     *
     * @param remoteAddress the remote IP address of the connecting client
     * @param cause the handshake failure cause (may be null)
     * @param sslSession the SSL session (may be null if handshake failed early)
     */
    void handleHandshakeFailure(String remoteAddress, Throwable cause, SSLSession sslSession) {
        String chargePointId = extractCpIdentityFromCert(sslSession)
        String reason = cause?.message ?: "Unknown handshake failure"

        def event = [
            chargePointId: chargePointId,
            type         : "InvalidChargePointCertificate",
            timestamp    : Instant.now().toString(),
            reason       : reason,
            sourceIp     : remoteAddress
        ]

        try {
            def jsonPayload = objectMapper.writeValueAsString(event)
            incoming.securityEvent(edgeConfig.kafkaNodeId, chargePointId,
                "InvalidChargePointCertificate", jsonPayload)
        } catch (Exception e) {
            logger.warn("Failed to publish InvalidChargePointCertificate to Kafka for {} ({}) — {}",
                chargePointId, remoteAddress, e.message)
        }

        logger.warn("InvalidChargePointCertificate from {} ({}): {}", chargePointId, remoteAddress, reason)
    }

    /**
     * Extracts charge point identity from the client certificate CN field.
     * Per OCPP 1.6 Security Whitepaper §3.3, the CN contains the charge point identity.
     * Returns "unknown" if extraction fails (no cert, malformed DN, no CN field).
     *
     * @param sslSession the SSL session containing peer certificates
     * @return charge point identity or "unknown"
     */
    String extractCpIdentityFromCert(SSLSession sslSession) {
        if (sslSession == null) {
            return "unknown"
        }

        try {
            def peerCerts = sslSession.getPeerCertificates()
            if (peerCerts == null || peerCerts.length == 0) {
                return "unknown"
            }

            X509Certificate clientCert = (X509Certificate) peerCerts[0]
            String dn = clientCert.getSubjectX500Principal().getName()
            def ldapName = new LdapName(dn)
            def cnRdn = ldapName.getRdns().find { it.type.equalsIgnoreCase("CN") }
            return cnRdn?.value?.toString() ?: "unknown"
        } catch (SSLPeerUnverifiedException e) {
            return "unknown"
        } catch (Exception e) {
            logger.debug("Failed to extract CN from client certificate: {}", e.message)
            return "unknown"
        }
    }
}

