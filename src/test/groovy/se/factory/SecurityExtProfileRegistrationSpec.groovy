package se.factory

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.feature.profile.securityext.ServerSecurityExtProfile
import se.EdgeConfig
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.SecurityExtEH
import se.service.ConnectionService
import spock.lang.Specification

/**
 * US-CERT105-01 Step 2: Verify SecurityExtProfile registration in factories.
 *
 * Tests that ServerSecurityExtProfile can be instantiated with SecurityExtEH.
 * Full factory integration tests are skipped due to keystore/SSL ConfigurationException
 * in test environment (pre-existing issue).
 */
class SecurityExtProfileRegistrationSpec extends Specification {

    def "ServerSecurityExtProfile can be created with SecurityExtEH handler"() {
        given: "A SecurityExtEH handler instance"
        def connections = Mock(ConnectionService)
        def incoming = Mock(OcppEventReceiver)
        def edgeConfig = Mock(EdgeConfig)
        edgeConfig.kafkaNodeId >> "000"
        def handler = new SecurityExtEH(connections, incoming, new ObjectMapper(), edgeConfig)

        when: "Creating ServerSecurityExtProfile with the handler"
        def profile = new ServerSecurityExtProfile(handler)

        then: "Profile is created successfully"
        profile != null

        and: "Profile has SecurityExt features registered"
        def features = profile.featureList
        features != null
        features.length > 0
    }

    def "ServerSecurityExtProfile feature list includes InstallCertificate, DeleteCertificate, GetInstalledCertificateIds"() {
        given: "A ServerSecurityExtProfile"
        def connections = Mock(ConnectionService)
        def incoming = Mock(OcppEventReceiver)
        def edgeConfig = Mock(EdgeConfig)
        edgeConfig.kafkaNodeId >> "000"
        def handler = new SecurityExtEH(connections, incoming, new ObjectMapper(), edgeConfig)
        def profile = new ServerSecurityExtProfile(handler)

        when: "Listing the features"
        def featureNames = profile.featureList*.class.simpleName

        then: "SecurityExt features are present"
        featureNames.contains("InstallCertificateFeature")
        featureNames.contains("DeleteCertificateFeature")
        featureNames.contains("GetInstalledCertificateIdsFeature")
    }
}

