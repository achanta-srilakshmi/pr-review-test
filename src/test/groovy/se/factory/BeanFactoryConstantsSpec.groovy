package se.factory
import spock.lang.Specification
/**
 * Unit tests for BeanFactory TLS constants defined in US-TLS103-02.
 * Validates OCPP 1.6 Security Whitepaper Appendix A approved cipher suites
 * and allowed TLS protocol versions.
 */
class BeanFactoryConstantsSpec extends Specification {
    def "APPROVED_CIPHER_SUITES should contain exactly 6 OCPP-approved suites"() {
        expect:
        BeanFactory.APPROVED_CIPHER_SUITES.size() == 6
    }
    def "APPROVED_CIPHER_SUITES should include all ECDHE and RSA GCM suites"() {
        expect:
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384")
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384")
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_RSA_WITH_AES_128_GCM_SHA256")
        BeanFactory.APPROVED_CIPHER_SUITES.contains("TLS_RSA_WITH_AES_256_GCM_SHA256")
    }
    def "APPROVED_CIPHER_SUITES should NOT contain RC4 or 3DES or NULL or EXPORT"() {
        expect:
        BeanFactory.APPROVED_CIPHER_SUITES.every { suite ->
            !suite.contains("RC4") &&
            !suite.contains("3DES") &&
            !suite.contains("NULL") &&
            !suite.contains("EXPORT") &&
            !suite.contains("DES_CBC")
        }
    }
    def "ALLOWED_TLS_PROTOCOLS should contain only TLS 1.2 and TLS 1.3"() {
        expect:
        BeanFactory.ALLOWED_TLS_PROTOCOLS.length == 2
        BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("TLSv1.2")
        BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("TLSv1.3")
    }
    def "ALLOWED_TLS_PROTOCOLS should NOT contain deprecated protocols"() {
        expect:
        !BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("TLSv1")
        !BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("TLSv1.0")
        !BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("TLSv1.1")
        !BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("SSLv3")
        !BeanFactory.ALLOWED_TLS_PROTOCOLS.contains("SSLv2")
    }
}