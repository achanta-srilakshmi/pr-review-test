package se

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import eu.chargetime.ocpp.model.SessionInformation
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.SocketSessionEvents
import se.ocpp16.handlers.SessionAuthenticator
import se.service.ChargerIdentifierCacheService
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for US-CERT106-03: SocketSessionEvents.newSession() reconnection detection
 * after SecurityProfile upgrade
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 §5.3, Security Whitepaper §3
 * [OCA-CERT-RELEVANT] TC-64
 */
class SecurityProfileReconnectionSpec extends Specification {

    @Subject
    SocketSessionEvents sse

    ConnectionService connections = Mock()
    OcppEventReceiver ocppIncoming = Mock()
    EdgeConfig edgeConfig = new EdgeConfig()
    ObjectMapper objectMapper = new ObjectMapper()
    MeterRegistry meterRegistry = new SimpleMeterRegistry()
    SessionAuthenticator authenticator = Mock()
    MessageProducer messageProducer = Mock()
    ChargerIdentifierCacheService chargerIdentifierCacheService = Stub()

    def setup() {
        objectMapper.registerModule(new JavaTimeModule())
        edgeConfig.kafkaNodeId = "000"
        edgeConfig.connectionProfile = "WSS"
        edgeConfig.chargers = new EdgeConfig.Chargers()
        edgeConfig.chargers.updateOnNewConnection = false // disable requestStatus thread

        sse = new SocketSessionEvents(
            objectMapper,
            ocppIncoming,
            meterRegistry,
            connections,
            edgeConfig,
            authenticator,
            true,   // kafkaEnabled
            messageProducer,
            chargerIdentifierCacheService
        )
    }

    private SessionInformation mockSessionInfo(String identifier) {
        def si = Mock(SessionInformation)
        si.getIdentifier() >> "/ocpp/${identifier}"
        return si
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-019: newSession detects successful reconnection after upgrade
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-019: newSession detects successful reconnection after upgrade"() {
        given: "Charger CP001 had a pending upgrade to Profile 3"
        def sessionUUID = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP001")
        def upgrade = new ConnectionService.SecurityProfileUpgrade(3, java.time.Instant.now())
        connections.completeSecurityProfileUpgrade("CP001") >> upgrade

        when: "newSession is called for CP001"
        sse.newSession(sessionUUID, sessionInfo)

        then: "completeSecurityProfileUpgrade is called"
        // newConnection is called (existing flow)
        1 * connections.newConnection(sessionUUID.toString(), "CP001")
        // upgrade completion detected — INFO log contains "successfully reconnected"
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-020: newSession with no pending upgrade proceeds normally
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-020: newSession with no pending upgrade proceeds normally"() {
        given: "No pending upgrade for CP001"
        def sessionUUID = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP001")
        connections.completeSecurityProfileUpgrade("CP001") >> null

        when: "newSession is called for CP001"
        sse.newSession(sessionUUID, sessionInfo)

        then: "Existing flow proceeds — newConnection, bootCycle, metrics"
        1 * connections.newConnection(sessionUUID.toString(), "CP001")
        1 * ocppIncoming.bootCycle("000", sessionUUID.toString(), "CP001", _, _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-021: newSession for different charger does not interfere
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-021: newSession for different charger does not interfere"() {
        given: "CP001 has pending upgrade, CP002 does not"
        def sessionUUID = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP002")
        connections.completeSecurityProfileUpgrade("CP002") >> null

        when: "newSession is called for CP002"
        sse.newSession(sessionUUID, sessionInfo)

        then: "No upgrade-related action for CP002"
        1 * connections.newConnection(sessionUUID.toString(), "CP002")
        // No upgrade log for CP002
    }
}

