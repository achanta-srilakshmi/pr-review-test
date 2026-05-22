package se.ocpp16

import org.java_websocket.enums.HandshakeState
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.handshake.HandshakedataImpl1
import org.java_websocket.handshake.ClientHandshakeBuilder
import se.factory.BeanFactory
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit tests for OcppSubprotocolDraft — OCPP 1.6 WebSocket subprotocol negotiation.
 * Covers US-SEC104-01 test cases: TC-SEC104-01-001 through TC-SEC104-01-018.
 */
class OcppSubprotocolDraftSpec extends Specification {

    // --- TC-SEC104-01-016: OCPP16_SUBPROTOCOL constant ---

    def "BeanFactory.OCPP16_SUBPROTOCOL constant equals 'ocpp1.6'"() {
        expect:
        BeanFactory.OCPP16_SUBPROTOCOL == "ocpp1.6"
    }

    // --- TC-SEC104-01-001: Accept ocpp1.6 — permissive mode ---

    def "should accept ocpp1.6 subprotocol in permissive mode"() {
        given: "a draft in permissive mode"
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.6")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-002: Accept ocpp1.6 — strict mode ---

    def "should accept ocpp1.6 subprotocol in strict mode"() {
        given: "a draft in strict mode"
        def draft = new OcppSubprotocolDraft(true)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.6")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-003: Reject unsupported ocpp1.5 ---

    def "should reject unsupported subprotocol ocpp1.5"() {
        given: "a draft in permissive mode"
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.5")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-004: Accept multiple with ocpp1.6 present ---

    def "should accept multiple subprotocols when ocpp1.6 is present"() {
        given: "a draft in permissive mode"
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.6, ocpp2.0.1")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-005: Permissive mode — accept missing header ---

    def "should accept connection without header in permissive mode"() {
        given: "a draft in permissive mode and no Sec-WebSocket-Protocol header"
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", null)

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-006: Strict mode — reject missing header ---

    def "should reject connection without header in strict mode"() {
        given: "a draft in strict mode and no Sec-WebSocket-Protocol header"
        def draft = new OcppSubprotocolDraft(true)
        def handshake = buildClientHandshake("/EVB-1", null)

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-007: Strict mode — reject unsupported ---

    def "should reject unsupported subprotocol in strict mode"() {
        given: "a draft in strict mode"
        def draft = new OcppSubprotocolDraft(true)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.5")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-008: Reject wrong case OCPP1.6 ---

    def "should reject wrong case OCPP1.6 — case-sensitive match"() {
        given:
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "OCPP1.6")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-009: Reject wrong case Ocpp1.6 ---

    def "should reject wrong case Ocpp1.6 — case-sensitive match"() {
        given:
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "Ocpp1.6")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-012: Handle whitespace around comma-separated values ---

    def "should handle whitespace around comma-separated values"() {
        given:
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", " ocpp1.6 , ocpp2.0.1 ")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-013: Handle trailing comma ---

    def "should handle trailing comma in subprotocol list"() {
        given:
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.6,")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.MATCHED
    }

    // --- TC-SEC104-01-014: Reject multiple unsupported protocols ---

    def "should reject when no supported protocol in multiple values"() {
        given:
        def draft = new OcppSubprotocolDraft(false)
        def handshake = buildClientHandshake("/EVB-1", "ocpp1.5, ocpp2.0.1")

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == HandshakeState.NOT_MATCHED
    }

    // --- TC-SEC104-01-015: copyInstance preserves strict mode ---

    def "copyInstance should preserve strict mode true"() {
        given:
        def draft = new OcppSubprotocolDraft(true)

        when:
        def copy = draft.copyInstance()

        then:
        copy instanceof OcppSubprotocolDraft
        ((OcppSubprotocolDraft) copy).strictMode == true
    }

    def "copyInstance should preserve strict mode false"() {
        given:
        def draft = new OcppSubprotocolDraft(false)

        when:
        def copy = draft.copyInstance()

        then:
        copy instanceof OcppSubprotocolDraft
        ((OcppSubprotocolDraft) copy).strictMode == false
    }

    // --- TC-SEC104-01-017: SubprotocolConfig defaults to permissive ---

    def "SubprotocolConfig should default strict to false"() {
        when:
        def config = new se.EdgeConfig.SubprotocolConfig()

        then:
        config.strict == false
    }

    // --- Parameterised edge-case tests ---

    @Unroll
    def "subprotocol '#offered' in #mode mode should be #expectedState"() {
        given:
        def draft = new OcppSubprotocolDraft(strict)
        def handshake = buildClientHandshake("/CP-001", offered)

        when:
        def result = draft.acceptHandshakeAsServer(handshake)

        then:
        result == expectedState

        where:
        offered                    | strict | expectedState              | mode
        "ocpp1.6"                  | false  | HandshakeState.MATCHED     | "permissive"
        "ocpp1.6"                  | true   | HandshakeState.MATCHED     | "strict"
        "ocpp1.5"                  | false  | HandshakeState.NOT_MATCHED | "permissive"
        "ocpp1.5"                  | true   | HandshakeState.NOT_MATCHED | "strict"
        "OCPP1.6"                  | false  | HandshakeState.NOT_MATCHED | "permissive"
        "Ocpp1.6"                  | false  | HandshakeState.NOT_MATCHED | "permissive"
        "ocpp1.6, ocpp2.0.1"      | false  | HandshakeState.MATCHED     | "permissive"
        " ocpp1.6 , ocpp2.0.1 "   | false  | HandshakeState.MATCHED     | "permissive"
        "ocpp1.6,"                 | false  | HandshakeState.MATCHED     | "permissive"
        "ocpp1.5, ocpp2.0.1"      | false  | HandshakeState.NOT_MATCHED | "permissive"
        null                       | false  | HandshakeState.MATCHED     | "permissive"
        null                       | true   | HandshakeState.NOT_MATCHED | "strict"
    }

    // --- Helper: build a minimal valid WebSocket client handshake ---

    /**
     * Builds a ClientHandshake with the required WebSocket upgrade headers
     * and optionally sets the Sec-WebSocket-Protocol header.
     */
    private ClientHandshake buildClientHandshake(String path, String subprotocol) {
        def handshake = new org.java_websocket.handshake.HandshakeImpl1Client()
        handshake.setResourceDescriptor(path)

        // Required WebSocket handshake fields for Draft_6455 version check
        handshake.put("Upgrade", "websocket")
        handshake.put("Connection", "Upgrade")
        handshake.put("Sec-WebSocket-Version", "13")
        handshake.put("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")

        if (subprotocol != null) {
            handshake.put("Sec-WebSocket-Protocol", subprotocol)
        }

        return handshake
    }
}

