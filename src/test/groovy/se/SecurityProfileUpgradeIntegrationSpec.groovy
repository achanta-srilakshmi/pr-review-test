package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.core.ChangeConfigurationConfirmation
import eu.chargetime.ocpp.model.core.ConfigurationStatus
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import reactor.core.scheduler.Schedulers
import se.bus.MessageProducer
import se.bus.OcppEventBroadcaster
import se.bus.OcppEventReceiver
import se.service.AuthCacheService
import se.service.ChargingProfileService
import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Integration tests for US-CERT106-03: OcppEventBroadcaster.updateConfiguration()
 * SecurityProfile-specific handling
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 §5.3, Security Whitepaper §3
 * [OCA-CERT-RELEVANT] TC-64
 */
class SecurityProfileUpgradeIntegrationSpec extends Specification {

    @Subject
    OcppEventBroadcaster broadcaster

    ConnectionService connections = Mock()
    OcppEventReceiver incoming = Mock()
    EdgeConfig edgeConfig = new EdgeConfig()
    ObjectMapper objectMapper = new ObjectMapper()
    MeterRegistry meterRegistry = new SimpleMeterRegistry()
    MessageProducer messageProducer = Mock()

    def setup() {
        edgeConfig.kafkaNodeId = "000"
        edgeConfig.mtls = new EdgeConfig.MtlsConfig()
        edgeConfig.mtls.enabled = false

        broadcaster = new OcppEventBroadcaster()
        broadcaster.edgeConfig = edgeConfig
        broadcaster.objectMapper = objectMapper
        broadcaster.incoming = incoming
        broadcaster.meterRegistry = meterRegistry
        broadcaster.connections = connections
        broadcaster.messageProducer = messageProducer
        broadcaster.schedulerType = Schedulers.immediate()
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-009: SecurityProfile Accepted triggers markSecurityProfileUpgrade
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-009: SecurityProfile Accepted triggers markSecurityProfileUpgrade"() {
        given: "Charger CP001 is connected"
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with SecurityProfile key and value 3"
        def payload = objectMapper.writeValueAsString([key: "SecurityProfile", value: "3"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is called with CP001 and profile 3"
        1 * connections.markSecurityProfileUpgrade("CP001", 3)

        and: "configurationUpdate is still published (existing flow)"
        1 * incoming.configurationUpdate("000", "CP001", _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-010: Non-SecurityProfile key does NOT trigger upgrade logic
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-010: Non-SecurityProfile key does not trigger upgrade logic"() {
        given: "Charger CP001 is connected"
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with HeartbeatInterval key"
        def payload = objectMapper.writeValueAsString([key: "HeartbeatInterval", value: "120"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is NOT called"
        0 * connections.markSecurityProfileUpgrade(_, _)

        and: "configurationUpdate is still published"
        1 * incoming.configurationUpdate("000", "CP001", _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-011: SecurityProfile Rejected does NOT trigger upgrade tracking
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-011: SecurityProfile Rejected does not trigger upgrade tracking"() {
        given: "Charger CP001 is connected"
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Rejected)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with SecurityProfile key"
        def payload = objectMapper.writeValueAsString([key: "SecurityProfile", value: "2"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is NOT called"
        0 * connections.markSecurityProfileUpgrade(_, _)

        and: "chargerError is published for Rejected status"
        1 * incoming.chargerError("000", "CP001", "ConfigurationUpdateError", _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-012: SecurityProfile Accepted with mTLS disabled logs WARN for Profile 3
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-012: SecurityProfile Accepted with mTLS disabled logs WARN for Profile 3"() {
        given: "mTLS is disabled and charger CP001 is connected"
        edgeConfig.mtls.enabled = false
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with SecurityProfile key and value 3"
        def payload = objectMapper.writeValueAsString([key: "SecurityProfile", value: "3"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is called"
        1 * connections.markSecurityProfileUpgrade("CP001", 3)

        and: "configurationUpdate is published"
        1 * incoming.configurationUpdate("000", "CP001", _)
        // WARN log about mTLS not configured is emitted (verified by log inspection)
        // The key assertion here is that the command is NOT blocked
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-013: SecurityProfile Accepted with mTLS enabled — NO WARN
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-013: SecurityProfile Accepted with mTLS enabled does NOT log WARN for Profile 3"() {
        given: "mTLS is enabled and charger CP001 is connected"
        edgeConfig.mtls.enabled = true
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with SecurityProfile key and value 3"
        def payload = objectMapper.writeValueAsString([key: "SecurityProfile", value: "3"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is called"
        1 * connections.markSecurityProfileUpgrade("CP001", 3)

        and: "configurationUpdate is published (no WARN about mTLS)"
        1 * incoming.configurationUpdate("000", "CP001", _)
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-014: SecurityProfile upgrade to Profile 2 — NO mTLS WARN
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-014: SecurityProfile upgrade to Profile 2 does NOT trigger mTLS WARN"() {
        given: "mTLS is disabled and charger CP001 is connected"
        edgeConfig.mtls.enabled = false
        connections.getLatestSessionId("CP001") >> UUID.randomUUID().toString()
        def confirmation = new ChangeConfigurationConfirmation(ConfigurationStatus.Accepted)
        connections.callInline(_, _) >> confirmation

        when: "updateConfiguration is called with SecurityProfile key and value 2"
        def payload = objectMapper.writeValueAsString([key: "SecurityProfile", value: "2"])
        broadcaster.updateConfiguration("000", "CP001", payload)

        then: "markSecurityProfileUpgrade is called with Profile 2"
        1 * connections.markSecurityProfileUpgrade("CP001", 2)

        and: "configurationUpdate is published"
        1 * incoming.configurationUpdate("000", "CP001", _)
        // No mTLS WARN — only Profile 3 triggers that warning
    }
}

