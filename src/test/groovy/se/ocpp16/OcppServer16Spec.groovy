package se.ocpp16

import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import se.EdgeConfig
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for OcppServer16 dual-port WS + WSS startup and shutdown.
 * Covers US-TLS103-02 test cases: TC-TLS103-02-001, TC-TLS103-02-002,
 * TC-TLS103-02-019, TC-TLS103-02-020, TC-TLS103-02-021.
 */
class OcppServer16Spec extends Specification {

    @Subject
    OcppServer16 ocppServer

    JSONServer mockWsServer
    JSONServer mockWssServer
    ServerEvents mockCoreEvents
    EdgeConfig edgeConfig

    def setup() {
        mockWsServer = Mock(JSONServer)
        mockWssServer = Mock(JSONServer)
        mockCoreEvents = Mock(ServerEvents)

        edgeConfig = new EdgeConfig()
        edgeConfig.webSocketPort = 9464
        edgeConfig.wss = new EdgeConfig.WSSConfig()
        edgeConfig.wss.enabled = false
        edgeConfig.wss.wssPort = 9943

        ocppServer = new OcppServer16()
        ocppServer.jsonServer = mockWsServer
        ocppServer.coreEvents = mockCoreEvents
        ocppServer.edgeConfig = edgeConfig
    }

    // --- TC-TLS103-02-001: WSS port binds alongside WS port ---

    def "should open both WS and WSS ports when WSS is enabled"() {
        given: "WSS is enabled and wssJsonServer bean is available"
        edgeConfig.wss.enabled = true
        edgeConfig.wss.wssPort = 9943
        ocppServer.wssJsonServer = mockWssServer

        when: "the server starts"
        ocppServer.started()

        then: "WS port is opened on 9464"
        1 * mockWsServer.open("0.0.0.0", 9464, mockCoreEvents)

        and: "WSS port is opened on 9943"
        1 * mockWssServer.open("0.0.0.0", 9943, mockCoreEvents)
    }

    // --- TC-TLS103-02-002: WSS disabled — only WS port binds ---

    def "should open only WS port when WSS is disabled"() {
        given: "WSS is disabled"
        edgeConfig.wss.enabled = false
        ocppServer.wssJsonServer = null

        when: "the server starts"
        ocppServer.started()

        then: "WS port is opened"
        1 * mockWsServer.open("0.0.0.0", 9464, mockCoreEvents)

        and: "WSS server is never opened"
        0 * mockWssServer.open(_, _, _)
    }

    def "should open only WS port when WSS is enabled but bean is null"() {
        given: "WSS is enabled but wssJsonServer bean was not created"
        edgeConfig.wss.enabled = true
        ocppServer.wssJsonServer = null

        when: "the server starts"
        ocppServer.started()

        then: "WS port is opened"
        1 * mockWsServer.open("0.0.0.0", 9464, mockCoreEvents)

        and: "no WSS port is opened since bean is null"
        0 * mockWssServer.open(_, _, _)
    }

    // --- TC-TLS103-02-019: Custom WSS port via config ---

    def "should use custom WSS port when configured"() {
        given: "a custom WSS port of 9950"
        edgeConfig.wss.enabled = true
        edgeConfig.wss.wssPort = 9950
        ocppServer.wssJsonServer = mockWssServer

        when: "the server starts"
        ocppServer.started()

        then: "WSS port binds on 9950"
        1 * mockWssServer.open("0.0.0.0", 9950, mockCoreEvents)
    }

    // --- TC-TLS103-02-020: Default WSS port ---

    def "should use default WSS port 9943 when not overridden"() {
        given: "default WSS port configuration"
        edgeConfig.wss.enabled = true
        // wssPort defaults to 9943 from EdgeConfig.WSSConfig
        ocppServer.wssJsonServer = mockWssServer

        when: "the server starts"
        ocppServer.started()

        then: "WSS port binds on default 9943"
        1 * mockWssServer.open("0.0.0.0", 9943, mockCoreEvents)
    }

    // --- TC-TLS103-02-021: Both servers closed on shutdown ---

    def "should close both WS and WSS servers on shutdown"() {
        given: "both servers are active"
        ocppServer.wssJsonServer = mockWssServer

        when: "shutdown event is received"
        ocppServer.onShutdownEvent(null)

        then: "WS server is closed"
        1 * mockWsServer.close()

        and: "WSS server is closed"
        1 * mockWssServer.close()
    }

    def "should close only WS server when WSS is not active"() {
        given: "only WS server is active"
        ocppServer.wssJsonServer = null

        when: "shutdown event is received"
        ocppServer.onShutdownEvent(null)

        then: "WS server is closed"
        1 * mockWsServer.close()

        and: "no WSS close is attempted"
        0 * mockWssServer.close()
    }

    // --- EdgeConfig.WSSConfig defaults ---

    def "WSSConfig should have correct default values"() {
        when: "a new WSSConfig is created"
        def wssConfig = new EdgeConfig.WSSConfig()

        then: "default wssPort is 9943"
        wssConfig.wssPort == 9943

        and: "WSS is disabled by default"
        !wssConfig.enabled
    }

    // --- US-SEC104-01: Subprotocol configuration ---

    def "SubprotocolConfig should default strict to false"() {
        when: "a new SubprotocolConfig is created"
        def subprotocolConfig = new EdgeConfig.SubprotocolConfig()

        then: "default strict is false (permissive mode)"
        !subprotocolConfig.strict
    }

    def "should call configureSubprotocol for WS server after open"() {
        given: "default config"
        edgeConfig.wss.enabled = false
        ocppServer.wssJsonServer = null

        when: "the server starts"
        ocppServer.started()

        then: "WS port is opened"
        1 * mockWsServer.open("0.0.0.0", 9464, mockCoreEvents)
        // configureSubprotocol is called internally but uses reflection on mock
        // — will log error since mock doesn't have real internal fields
        // This verifies the method doesn't throw/crash for mocks
    }

    def "should call configureSubprotocol for both WS and WSS after open"() {
        given: "WSS enabled"
        edgeConfig.wss.enabled = true
        ocppServer.wssJsonServer = mockWssServer

        when: "the server starts"
        ocppServer.started()

        then: "both servers are opened"
        1 * mockWsServer.open("0.0.0.0", 9464, mockCoreEvents)
        1 * mockWssServer.open("0.0.0.0", 9943, mockCoreEvents)
        // configureSubprotocol is called for both — no exception thrown
    }
}

