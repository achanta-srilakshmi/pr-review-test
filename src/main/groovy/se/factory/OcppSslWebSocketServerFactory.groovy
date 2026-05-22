package se.factory

import org.java_websocket.SSLSocketChannel2
import org.java_websocket.server.DefaultSSLWebSocketServerFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import java.nio.channels.ByteChannel
import java.nio.channels.SelectionKey
import java.nio.channels.SocketChannel

/**
 * WebSocketServerFactory that enforces OCPP 1.6 Security Profile 2 TLS requirements:
 * - TLS 1.2+ only (ALLOWED_TLS_PROTOCOLS)
 * - OCPP-approved cipher suites only (APPROVED_CIPHER_SUITES)
 *
 * Applied to all WSS connections regardless of mTLS mode.
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper Section 3.1.2, Appendix A
 * @since US-TLS103-02 (TC-66)
 */
class OcppSslWebSocketServerFactory extends DefaultSSLWebSocketServerFactory {

    private static final Logger logger = LoggerFactory.getLogger(OcppSslWebSocketServerFactory)

    OcppSslWebSocketServerFactory(SSLContext sslContext) {
        super(sslContext)
        logger.info("OcppSslWebSocketServerFactory created — enforcing TLS 1.2+ and OCPP-approved cipher suites")
    }

    @Override
    ByteChannel wrapChannel(SocketChannel channel, SelectionKey key) throws IOException {
        SSLEngine engine = sslcontext.createSSLEngine()
        engine.setUseClientMode(false)

        // Enforce OCPP-approved cipher suites
        String[] supportedCiphers = engine.getSupportedCipherSuites()
        String[] approvedCiphers = supportedCiphers.findAll { cipher ->
            BeanFactory.APPROVED_CIPHER_SUITES.contains(cipher)
        } as String[]

        if (approvedCiphers.length > 0) {
            engine.setEnabledCipherSuites(approvedCiphers)
        } else {
            logger.warn("No OCPP-approved cipher suites supported by JVM — falling back to defaults")
        }

        // Enforce TLS 1.2+ only
        engine.setEnabledProtocols(BeanFactory.ALLOWED_TLS_PROTOCOLS)

        logger.trace("SSLEngine configured — protocols={}, ciphers={}", engine.getEnabledProtocols(), engine.getEnabledCipherSuites())

        engine.beginHandshake()
        return new SSLSocketChannel2(channel, engine, exec, key)
    }
}

