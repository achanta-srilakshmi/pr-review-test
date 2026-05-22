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
 * Unit tests for the charger identifier cache gate in SocketSessionEvents.newSession().
 * Covers: TC-02-002, TC-02-003, TC-02-004, TC-02-017
 */
class SocketSessionEventsGateSpec extends Specification {

    @Subject
    SocketSessionEvents sse

    ConnectionService connections = Mock()
    OcppEventReceiver ocppIncoming = Mock()
    EdgeConfig edgeConfig = new EdgeConfig()
    ObjectMapper objectMapper = new ObjectMapper()
    MeterRegistry meterRegistry = new SimpleMeterRegistry()
    SessionAuthenticator authenticator = Mock()
    MessageProducer messageProducer = Mock()
    ChargerIdentifierCacheService cacheService = Mock()

    def setup() {
        objectMapper.registerModule(new JavaTimeModule())
        edgeConfig.kafkaNodeId = "node-test"
        edgeConfig.connectionProfile = "WS"
        edgeConfig.chargers = new EdgeConfig.Chargers()
        edgeConfig.chargers.updateOnNewConnection = false   // disable requestStatus thread
        edgeConfig.cacheValidation = new EdgeConfig.CacheValidationConfig()

        sse = new SocketSessionEvents(
            objectMapper,
            ocppIncoming,
            meterRegistry,
            connections,
            edgeConfig,
            authenticator,
            true,
            messageProducer,
            cacheService
        )
    }

    private SessionInformation mockSessionInfo(String identifier) {
        def si = Mock(SessionInformation)
        si.getIdentifier() >> "/ocpp/${identifier}"
        return si
    }

    // -----------------------------------------------------------------------
    // TC-02-002: Gate completely skipped when chargerIdCheck=false
    // Even an identifier that is NOT in cache proceeds normally
    // -----------------------------------------------------------------------
    def "TC-02-002: gate skipped when chargerIdCheck=false — unknown charger connects normally"() {
        given: "enforcement disabled"
        edgeConfig.cacheValidation.chargerIdCheck = false

        and: "charger is NOT in cache"
        def sessionId = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP-UNKNOWN")

        when: "charger opens WebSocket"
        sse.newSession(sessionId, sessionInfo)

        then: "closeSession is never called"
        0 * connections.closeSession(_)

        and: "bootCycle Kafka event IS published"
        1 * ocppIncoming.bootCycle(_, _, "CP-UNKNOWN", _, _)

        and: "no interaction with cache service for authorization"
        0 * cacheService.isAuthorized(_)
    }

    // -----------------------------------------------------------------------
    // TC-02-003: Gate open — chargerIdCheck=true, charger IS in cache
    // -----------------------------------------------------------------------
    def "TC-02-003: gate open for authorized charger when chargerIdCheck=true"() {
        given: "enforcement active; cache loaded; CP-REGISTERED is authorized"
        edgeConfig.cacheValidation.chargerIdCheck = true
        cacheService.isCacheLoaded() >> true
        cacheService.isAuthorized("CP-REGISTERED") >> true

        def sessionId = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP-REGISTERED")

        when: "charger opens WebSocket"
        sse.newSession(sessionId, sessionInfo)

        then: "closeSession is NOT called"
        0 * connections.closeSession(_)

        and: "bootCycle Kafka event IS published"
        1 * ocppIncoming.bootCycle(_, _, "CP-REGISTERED", _, _)
    }

    // -----------------------------------------------------------------------
    // TC-02-004: Gate closed — chargerIdCheck=true, charger NOT in cache
    // -----------------------------------------------------------------------
    def "TC-02-004: gate closes session when chargerIdCheck=true and charger not in cache"() {
        given: "enforcement active; cache loaded; CP-GHOST is NOT authorized"
        edgeConfig.cacheValidation.chargerIdCheck = true
        cacheService.isCacheLoaded() >> true
        cacheService.isAuthorized("CP-GHOST") >> false

        def sessionId = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP-GHOST")

        when: "unregistered charger opens WebSocket"
        sse.newSession(sessionId, sessionInfo)

        then: "closeSession is called with the correct sessionIndex"
        1 * connections.closeSession(sessionId)

        and: "NO bootCycle Kafka event published — early return before Kafka call"
        0 * ocppIncoming.bootCycle(_, _, _, _, _)

        and: "no new connection registered"
        0 * connections.newConnection(_, _)
    }

    // -----------------------------------------------------------------------
    // TC-02-017: Gate skipped when chargerIdCheck=true but cache NOT yet loaded
    // K8s readiness probe returns DOWN in this state — so we should NOT close sessions here
    // Gate condition: chargerIdCheck && cacheLoaded && !isAuthorized
    // -----------------------------------------------------------------------
    def "TC-02-017: gate skipped when cache not yet loaded even if chargerIdCheck=true"() {
        given: "enforcement active; cache NOT yet loaded (first scheduler tick pending)"
        edgeConfig.cacheValidation.chargerIdCheck = true
        cacheService.isCacheLoaded() >> false

        def sessionId = UUID.randomUUID()
        def sessionInfo = mockSessionInfo("CP-ANY")

        when: "charger connects before cache is warm"
        sse.newSession(sessionId, sessionInfo)

        then: "closeSession is NOT called — readiness probe handles traffic prevention at infra layer"
        0 * connections.closeSession(_)

        and: "connection proceeds normally"
        1 * ocppIncoming.bootCycle(_, _, "CP-ANY", _, _)
    }
}
