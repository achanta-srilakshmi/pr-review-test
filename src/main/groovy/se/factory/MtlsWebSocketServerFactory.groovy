package se.factory

import org.java_websocket.SSLSocketChannel2
import org.java_websocket.server.DefaultSSLWebSocketServerFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.service.MtlsHandshakeListener

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import java.nio.channels.ByteChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel

/**
 * Custom WebSocketServerFactory that configures client certificate authentication
 * (clientAuth) on each SSLEngine created for incoming WSS connections.
 *
 * Extends DefaultSSLWebSocketServerFactory to override wrapChannel() and set
 * needClientAuth/wantClientAuth based on the configured clientAuth mode.
 *
 * US-CERT106-01: mTLS Infrastructure (TC-70, TC-69)
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3.3 — Security Profile 3
 */
class MtlsWebSocketServerFactory extends DefaultSSLWebSocketServerFactory {

    private static final Logger logger = LoggerFactory.getLogger(MtlsWebSocketServerFactory)

    private final String clientAuth
    private final MtlsHandshakeListener handshakeListener

    /**
     * @param sslContext SSLContext with truststore-backed TrustManagerFactory
     * @param clientAuth "REQUIRE" for strict Profile 3, "OPTIONAL" for Profile 2+3 coexistence
     * @param handshakeListener listener for publishing InvalidChargePointCertificate events on failure
     */
    MtlsWebSocketServerFactory(SSLContext sslContext, String clientAuth, MtlsHandshakeListener handshakeListener) {
        super(sslContext)
        this.clientAuth = clientAuth
        this.handshakeListener = handshakeListener
        logger.info("MtlsWebSocketServerFactory created — clientAuth={}", clientAuth)
    }

    /**
     * Wraps a SocketChannel with SSL, configuring clientAuth on the SSLEngine.
     * When clientAuth=REQUIRE, setNeedClientAuth(true) — client MUST present a certificate.
     * When clientAuth=OPTIONAL, setWantClientAuth(true) — client MAY present a certificate.
     *
     * On SSL setup failure, delegates to MtlsHandshakeListener to publish SecurityEvent.
     */
    @Override
    ByteChannel wrapChannel(SocketChannel channel, SelectionKey key) throws IOException {
        SSLEngine engine = sslcontext.createSSLEngine()
        engine.setUseClientMode(false)

        // Enforce OCPP-approved cipher suites (TC-66)
        String[] supportedCiphers = engine.getSupportedCipherSuites()
        String[] approvedCiphers = supportedCiphers.findAll { cipher ->
            BeanFactory.APPROVED_CIPHER_SUITES.contains(cipher)
        } as String[]
        if (approvedCiphers.length > 0) {
            engine.setEnabledCipherSuites(approvedCiphers)
        }

        // Enforce TLS 1.2+ only (TC-66)
        engine.setEnabledProtocols(BeanFactory.ALLOWED_TLS_PROTOCOLS)

        if ("REQUIRE".equalsIgnoreCase(clientAuth)) {
            engine.setNeedClientAuth(true)
        } else {
            engine.setWantClientAuth(true)
        }

        try {
            engine.beginHandshake()
            return new SSLSocketChannel2(channel, engine, exec, key)
        } catch (Exception e) {
            // SSL setup failure — attempt to extract peer info and publish SecurityEvent
            String remoteAddress = channel?.socket()?.remoteSocketAddress?.toString() ?: "unknown"
            logger.warn("mTLS handshake setup failure from {}: {}", remoteAddress, e.message)

            if (handshakeListener != null) {
                try {
                    handshakeListener.handleHandshakeFailure(remoteAddress, e, engine.getSession())
                } catch (Exception listenerEx) {
                    logger.debug("Handshake listener error: {}", listenerEx.message)
                }
            }
            throw e instanceof IOException ? (IOException) e : new IOException("mTLS handshake failure", e)
        }
    }
}

