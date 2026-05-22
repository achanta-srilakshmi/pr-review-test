package se

import se.service.ConnectionService
import spock.lang.Specification
import spock.lang.Subject

import java.time.Instant

/**
 * Unit tests for US-CERT106-03: ConnectionService SecurityProfile upgrade tracking
 *
 * Tests markSecurityProfileUpgrade(), isUpgradingSecurityProfile(), completeSecurityProfileUpgrade()
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3 — Security Profile Upgrade
 * [OCA-CERT-RELEVANT] TC-64
 */
class SecurityProfileUpgradeSpec extends Specification {

    @Subject
    ConnectionService connectionService

    EdgeConfig edgeConfig = new EdgeConfig()

    def setup() {
        edgeConfig.mtls = new EdgeConfig.MtlsConfig()
        edgeConfig.mtls.upgradeGracePeriodSeconds = 60

        connectionService = new ConnectionService()
        connectionService.edgeConfig = edgeConfig
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-001: markSecurityProfileUpgrade stores pending upgrade
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-001: markSecurityProfileUpgrade stores pending upgrade"() {
        when: "markSecurityProfileUpgrade is called for CP001 targeting Profile 3"
        connectionService.markSecurityProfileUpgrade("CP001", 3)

        then: "pendingUpgrades contains entry for CP001 with targetProfile=3"
        connectionService.pendingUpgrades.containsKey("CP001")
        connectionService.pendingUpgrades.get("CP001").targetProfile == 3
        // Timestamp should be recent (within 1 second)
        def diff = java.time.Duration.between(connectionService.pendingUpgrades.get("CP001").timestamp, Instant.now())
        diff.seconds < 1
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-002: isUpgradingSecurityProfile returns true within grace period
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-002: isUpgradingSecurityProfile returns true within grace period"() {
        given: "markSecurityProfileUpgrade was called for CP001"
        connectionService.markSecurityProfileUpgrade("CP001", 3)

        when: "isUpgradingSecurityProfile is called immediately"
        def result = connectionService.isUpgradingSecurityProfile("CP001")

        then: "Returns true and entry still present"
        result == true
        connectionService.pendingUpgrades.containsKey("CP001")
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-003: isUpgradingSecurityProfile returns false after grace period
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-003: isUpgradingSecurityProfile returns false after grace period expires"() {
        given: "Grace period is set to 1 second and upgrade was marked"
        edgeConfig.mtls.upgradeGracePeriodSeconds = 1
        connectionService.markSecurityProfileUpgrade("CP001", 3)

        and: "2 seconds elapse"
        Thread.sleep(2000)

        when: "isUpgradingSecurityProfile is called"
        def result = connectionService.isUpgradingSecurityProfile("CP001")

        then: "Returns false and entry is cleaned up"
        result == false
        !connectionService.pendingUpgrades.containsKey("CP001")
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-004: isUpgradingSecurityProfile returns false for unknown charger
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-004: isUpgradingSecurityProfile returns false for unknown charger"() {
        when: "isUpgradingSecurityProfile is called for an unknown charger"
        def result = connectionService.isUpgradingSecurityProfile("UNKNOWN_CP")

        then: "Returns false"
        result == false
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-005: completeSecurityProfileUpgrade removes entry and returns info
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-005: completeSecurityProfileUpgrade removes entry and returns upgrade info"() {
        given: "Upgrade was marked for CP001"
        connectionService.markSecurityProfileUpgrade("CP001", 3)

        when: "completeSecurityProfileUpgrade is called"
        def result = connectionService.completeSecurityProfileUpgrade("CP001")

        then: "Returns the upgrade with targetProfile=3 and removes entry"
        result != null
        result.targetProfile == 3
        !connectionService.pendingUpgrades.containsKey("CP001")
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-006: completeSecurityProfileUpgrade returns null for unknown
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-006: completeSecurityProfileUpgrade returns null for unknown charger"() {
        when: "completeSecurityProfileUpgrade is called for unknown charger"
        def result = connectionService.completeSecurityProfileUpgrade("UNKNOWN_CP")

        then: "Returns null"
        result == null
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-007: markSecurityProfileUpgrade overwrites existing entry
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-007: markSecurityProfileUpgrade overwrites existing entry"() {
        given: "Upgrade was marked for CP001 targeting Profile 2"
        connectionService.markSecurityProfileUpgrade("CP001", 2)

        when: "markSecurityProfileUpgrade is called again targeting Profile 3"
        connectionService.markSecurityProfileUpgrade("CP001", 3)

        then: "Entry is overwritten with Profile 3"
        connectionService.pendingUpgrades.get("CP001").targetProfile == 3
        connectionService.pendingUpgrades.size() == 1
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-008: Multiple chargers can have concurrent pending upgrades
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-008: Multiple chargers can have concurrent pending upgrades"() {
        when: "Upgrades are marked for CP001 and CP002"
        connectionService.markSecurityProfileUpgrade("CP001", 3)
        connectionService.markSecurityProfileUpgrade("CP002", 2)

        then: "Both entries exist independently"
        connectionService.pendingUpgrades.size() == 2
        connectionService.pendingUpgrades.get("CP001").targetProfile == 3
        connectionService.pendingUpgrades.get("CP002").targetProfile == 2
    }
}

