package se.ocpp16

import org.java_websocket.drafts.Draft
import org.java_websocket.drafts.Draft_6455
import org.java_websocket.enums.HandshakeState
import org.java_websocket.exceptions.InvalidHandshakeException
import org.java_websocket.extensions.IExtension
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.protocols.IProtocol
import org.java_websocket.protocols.Protocol
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.factory.BeanFactory

/**
 * Custom Draft_6455 implementing OCPP 1.6 WebSocket subprotocol negotiation.
 *
 * Per OCPP 1.6 Part 2 Section 3 and RFC 6455 §4.2.2:
 * - Validates Sec-WebSocket-Protocol header for case-sensitive "ocpp1.6" match
 * - Echoes "ocpp1.6" in HTTP 101 response when matched
 * - Rejects unsupported subprotocols (e.g. "ocpp1.5", "OCPP1.6")
 * - Configurable strict/permissive mode for missing header
 *
 * Strict mode (se.config.subprotocol.strict=true):
 *   - Requires Sec-WebSocket-Protocol: ocpp1.6 — rejects if missing or unsupported
 *
 * Permissive mode (default, strict=false):
 *   - Accepts ocpp1.6, rejects unsupported, allows missing header for backward compatibility
 *
 * @since US-SEC104-01 (TC-67)
 */
class OcppSubprotocolDraft extends Draft_6455 {

    private static final Logger logger = LoggerFactory.getLogger(OcppSubprotocolDraft.class)

    /**
     * Cached reflection field for Draft_6455.knownProtocols (private).
     * Used to temporarily swap protocols for permissive-mode acceptance.
     */
    private static final java.lang.reflect.Field KNOWN_PROTOCOLS_FIELD = initProtocolsField()

    final boolean strictMode

    /**
     * Creates a new OcppSubprotocolDraft.
     *
     * @param strictMode when true, reject connections without Sec-WebSocket-Protocol header;
     *                   when false (permissive), accept connections without the header
     */
    OcppSubprotocolDraft(boolean strictMode) {
        super(Collections.<IExtension> emptyList(),
              Collections.<IProtocol> singletonList(new Protocol(BeanFactory.OCPP16_SUBPROTOCOL)))
        this.strictMode = strictMode
    }

    /**
     * Internal constructor used by {@link #copyInstance()}.
     */
    private OcppSubprotocolDraft(List<IExtension> extensions, List<IProtocol> protocols,
                                  int maxFrameSize, boolean strictMode) {
        super(extensions, protocols, maxFrameSize)
        this.strictMode = strictMode
    }

    @Override
    HandshakeState acceptHandshakeAsServer(ClientHandshake handshakedata)
            throws InvalidHandshakeException {
        String path = handshakedata.getResourceDescriptor() ?: "unknown"
        boolean hasProtocol = handshakedata.hasFieldValue("Sec-WebSocket-Protocol")

        if (!hasProtocol) {
            // No Sec-WebSocket-Protocol header present (or empty value — Java-WebSocket
            // treats both identically via hasFieldValue returning false)
            if (strictMode) {
                logger.warn("Subprotocol strict mode: rejecting connection without " +
                    "Sec-WebSocket-Protocol header. path={}", path)
                return HandshakeState.NOT_MATCHED
            }
            // Permissive mode: accept without subprotocol.
            // Temporarily swap knownProtocols to Protocol("") which accepts any/empty input,
            // so the parent's WebSocket version and extension checks still run.
            return acceptPermissive(handshakedata)
        }

        // Header present with value — delegate to parent for protocol matching.
        // Protocol("ocpp1.6") handles case-sensitive match and comma-separated values.
        HandshakeState result = super.acceptHandshakeAsServer(handshakedata)
        if (result != HandshakeState.MATCHED) {
            String offered = handshakedata.getFieldValue("Sec-WebSocket-Protocol")
            logger.warn("Unsupported WebSocket subprotocol offered. path={}, offered='{}'",
                path, offered)
        }
        return result
    }

    /**
     * Accepts the handshake in permissive mode when no Sec-WebSocket-Protocol header is present.
     * Temporarily swaps knownProtocols to include Protocol("") so the parent's
     * version check and extension check pass without requiring a specific protocol match.
     *
     * Thread-safe: each connection gets its own Draft copy via {@link #copyInstance()}.
     */
    private HandshakeState acceptPermissive(ClientHandshake handshakedata)
            throws InvalidHandshakeException {
        if (KNOWN_PROTOCOLS_FIELD == null) {
            logger.error("Cannot apply permissive mode — knownProtocols field not accessible. " +
                "Falling back to strict rejection.")
            return HandshakeState.NOT_MATCHED
        }

        List<IProtocol> original = (List<IProtocol>) KNOWN_PROTOCOLS_FIELD.get(this)
        KNOWN_PROTOCOLS_FIELD.set(this, Collections.<IProtocol> singletonList(new Protocol("")))
        try {
            return super.acceptHandshakeAsServer(handshakedata)
        } finally {
            KNOWN_PROTOCOLS_FIELD.set(this, original)
        }
    }

    @Override
    Draft copyInstance() {
        List<IExtension> newExtensions = new ArrayList<>()
        for (IExtension ext : getKnownExtensions()) {
            newExtensions.add(ext.copyInstance())
        }
        List<IProtocol> newProtocols = new ArrayList<>()
        for (IProtocol proto : getKnownProtocols()) {
            newProtocols.add(proto.copyInstance())
        }
        return new OcppSubprotocolDraft(newExtensions, newProtocols, getMaxFrameSize(), this.strictMode)
    }

    @Override
    String toString() {
        return "OcppSubprotocolDraft[strict=${strictMode}, protocol=${BeanFactory.OCPP16_SUBPROTOCOL}]"
    }

    private static java.lang.reflect.Field initProtocolsField() {
        try {
            def field = Draft_6455.getDeclaredField('knownProtocols')
            field.setAccessible(true)
            return field
        } catch (NoSuchFieldException e) {
            logger.error("Failed to access Draft_6455.knownProtocols field — " +
                "permissive mode subprotocol negotiation will not work", e)
            return null
        }
    }
}

