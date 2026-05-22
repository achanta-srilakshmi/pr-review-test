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
 * Unit tests for US-CERT106-03: SocketSessionEvents LostSession suppression
 * and reconnection detection during SecurityProfile upgrade
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 §5.3, Security Whitepaper §3
 * [OCA-CERT-RELEVANT] TC-64
 */
class SecurityProfileLostSessionSpec extends Specification {

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

    // -----------------------------------------------------------------------
    // TC-CERT106-03-015: lostSession suppressed during active security profile upgrade
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-015: lostSession suppressed during active security profile upgrade"() {
        given: "Charger CP001 is upgrading security profile"
        def sessionUUID = UUID.randomUUID()
        connections.getChargerId(sessionUUID.toString()) >> "CP001"
        connections.isUpgradingSecurityProfile("CP001") >> true

        when: "lostSession is called"
        sse.lostSession(sessionUUID)

        then: "inactiveCharger is NOT published"
        0 * ocppIncoming.inactiveCharger(_, _, _, _)

        and: "lostConnection is NOT called"
        0 * connections.lostConnection(_)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-016: lostSession proceeds normally when no pending upgrade
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-016: lostSession proceeds normally when no pending upgrade"() {
        given: "Charger CP001 has no pending upgrade"
        def sessionUUID = UUID.randomUUID()
        connections.getChargerId(sessionUUID.toString()) >> "CP001"
        connections.isUpgradingSecurityProfile("CP001") >> false

        when: "lostSession is called"
        sse.lostSession(sessionUUID)

        then: "inactiveCharger IS published"
        1 * ocppIncoming.inactiveCharger("000", "CP001", _, _)

        and: "lostConnection IS called"
        1 * connections.lostConnection(sessionUUID.toString())
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-017: lostSession proceeds normally after grace period expires
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-017: lostSession proceeds normally after grace period expires"() {
        given: "Charger CP001 grace period has expired (isUpgrading returns false)"
        def sessionUUID = UUID.randomUUID()
        connections.getChargerId(sessionUUID.toString()) >> "CP001"
        connections.isUpgradingSecurityProfile("CP001") >> false

        when: "lostSession is called"
        sse.lostSession(sessionUUID)

        then: "inactiveCharger IS published (grace period expired)"
        1 * ocppIncoming.inactiveCharger("000", "CP001", _, _)

        and: "lostConnection IS called"
        1 * connections.lostConnection(sessionUUID.toString())
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-018: lostSession with null identifier does not trigger upgrade check
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-018: lostSession with null identifier does not trigger upgrade check"() {
        given: "Session has null identifier (unknown session)"
        def sessionUUID = UUID.randomUUID()
        connections.getChargerId(sessionUUID.toString()) >> null

        when: "lostSession is called"
        sse.lostSession(sessionUUID)

        then: "isUpgradingSecurityProfile is never called"
        0 * connections.isUpgradingSecurityProfile(_)

        and: "Existing null-handling proceeds"
        1 * ocppIncoming.inactiveCharger("000", null, _, _)
    }
}

