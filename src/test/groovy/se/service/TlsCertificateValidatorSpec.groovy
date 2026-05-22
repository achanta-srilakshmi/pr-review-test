package se.service
import se.EdgeConfig
import spock.lang.Specification
import spock.lang.Subject
class TlsCertificateValidatorSpec extends Specification {
    @Subject
    TlsCertificateValidator validator
    EdgeConfig edgeConfig
    def setup() {
        edgeConfig = new EdgeConfig()
        edgeConfig.wss = new EdgeConfig.WSSConfig()
    }
    def "should skip validation when WSS is disabled"() {
        given:
        edgeConfig.wss.enabled = false
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        noExceptionThrown()
    }
    def "should pass validation with valid keystore and non-expired certificate"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "src/test/resources/tls/test-server.p12"
        edgeConfig.wss.storePassword = "changeit"
        edgeConfig.wss.keyPassword = "changeit"
        edgeConfig.wss.alias = "evoke"
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        noExceptionThrown()
    }
    def "should fail-fast when keystore path is null"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = null
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.contains("TLS keystore path is not configured")
    }
    def "should fail-fast when keystore path is empty"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = ""
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.contains("TLS keystore path is not configured")
    }
    def "should fail-fast when keystore file does not exist"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "/nonexistent/path/keystore.p12"
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.contains("/nonexistent/path/keystore.p12")
    }
    def "should fail-fast when keystore password is incorrect"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "src/test/resources/tls/test-server.p12"
        edgeConfig.wss.storePassword = "wrongpassword"
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.contains("TLS keystore password is incorrect")
        !ex.message.contains("wrongpassword")
        !ex.message.contains("changeit")
    }
    def "should fail-fast when configured alias does not exist"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "src/test/resources/tls/test-server.p12"
        edgeConfig.wss.storePassword = "changeit"
        edgeConfig.wss.keyPassword = "changeit"
        edgeConfig.wss.alias = "nonexistent-alias"
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.contains("nonexistent-alias")
        ex.message.contains("evoke")
    }
    def "should pass when no alias configured and keystore has entries"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "src/test/resources/tls/test-server.p12"
        edgeConfig.wss.storePassword = "changeit"
        edgeConfig.wss.keyPassword = "changeit"
        edgeConfig.wss.alias = null
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        noExceptionThrown()
    }
    def "should fail-fast when certificate is expired"() {
        given:
        edgeConfig.wss.enabled = true
        edgeConfig.wss.storeType = "PKCS12"
        edgeConfig.wss.keyStore = "src/test/resources/tls/test-server-expired.p12"
        edgeConfig.wss.storePassword = "changeit"
        edgeConfig.wss.keyPassword = "changeit"
        edgeConfig.wss.alias = "evoke-expired"
        when:
        validator = new TlsCertificateValidator(edgeConfig)
        then:
        def ex = thrown(TlsCertificateValidator.TlsValidationException)
        ex.message.toLowerCase().contains("expired")
        ex.message.contains("localhost-expired")
    }
}
