package se

import spock.lang.Specification

/**
 * Unit tests for US-CERT106-03: EdgeConfig.MtlsConfig.upgradeGracePeriodSeconds
 *
 * [OCPP-COMPLIANCE] OCPP 1.6 Security Whitepaper §3 — Security Profile Upgrade
 * [OCA-CERT-RELEVANT] TC-64
 */
class SecurityProfileUpgradeConfigSpec extends Specification {

    // -----------------------------------------------------------------------
    // TC-CERT106-03-022: upgradeGracePeriodSeconds defaults to 60
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-022: upgradeGracePeriodSeconds defaults to 60"() {
        when: "MtlsConfig is instantiated with defaults"
        def mtls = new EdgeConfig.MtlsConfig()

        then: "upgradeGracePeriodSeconds == 60"
        mtls.upgradeGracePeriodSeconds == 60
    }

    // -----------------------------------------------------------------------
    // TC-CERT106-03-023: upgradeGracePeriodSeconds respects custom value
    // -----------------------------------------------------------------------
    def "TC-CERT106-03-023: upgradeGracePeriodSeconds respects custom value"() {
        when: "MtlsConfig is instantiated and value overridden"
        def mtls = new EdgeConfig.MtlsConfig()
        mtls.upgradeGracePeriodSeconds = 120

        then: "upgradeGracePeriodSeconds == 120"
        mtls.upgradeGracePeriodSeconds == 120
    }
}

