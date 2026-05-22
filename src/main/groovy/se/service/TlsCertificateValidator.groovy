package se.service

import io.micronaut.context.annotation.Context
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig

import java.security.KeyStore
import java.security.cert.X509Certificate

/**
 * Eagerly validates the TLS keystore at startup, before Netty binds any port.
 * If validation fails, the service exits immediately (fail-fast).
 *
 * This bean is @Context-scoped so it is constructed during application startup,
 * before any @PostConstruct methods run (including OcppServer16.started()).
 *
 * @since US-TLS103-01
 */
@Context
@Singleton
class TlsCertificateValidator {

    private static final Logger logger = LoggerFactory.getLogger(TlsCertificateValidator)

    /**
     * Custom exception for TLS validation failures.
     * Used instead of System.exit(1) to allow testability.
     * In production, this uncaught exception in the @Context constructor
     * will prevent the application from starting.
     */
    static class TlsValidationException extends RuntimeException {
        TlsValidationException(String message) {
            super(message)
        }
        TlsValidationException(String message, Throwable cause) {
            super(message, cause)
        }
    }

    TlsCertificateValidator(EdgeConfig edgeConfig) {
        if (!edgeConfig.wss.enabled) {
            logger.info("WSS is disabled — skipping TLS keystore validation")
            return
        }

        validateKeystorePath(edgeConfig)
        KeyStore keyStore = loadKeystore(edgeConfig)
        String resolvedAlias = validateKeyAlias(keyStore, edgeConfig)
        validateCertificateExpiry(keyStore, resolvedAlias)

        logger.info("TLS keystore validation passed — certificate is valid")

        // US-CERT106-01: Validate truststore when mTLS is enabled
        if (edgeConfig.mtls.enabled) {
            validateTruststore(edgeConfig)
            logger.info("mTLS truststore validation passed")
        }
    }

    private void validateKeystorePath(EdgeConfig edgeConfig) {
        String keystorePath = edgeConfig.wss.keyStore

        if (keystorePath == null || keystorePath.trim().isEmpty()) {
            String message = "TLS keystore path is not configured"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }

        File keystoreFile = new File(keystorePath)
        if (!keystoreFile.exists()) {
            String message = "TLS keystore file not found: ${keystorePath}"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }
    }

    private KeyStore loadKeystore(EdgeConfig edgeConfig) {
        try {
            KeyStore keyStore = KeyStore.getInstance(edgeConfig.wss.storeType ?: "PKCS12")
            File keystoreFile = new File(edgeConfig.wss.keyStore)
            FileInputStream fis = new FileInputStream(keystoreFile)
            try {
                keyStore.load(fis, edgeConfig.wss.storePassword?.toCharArray())
            } finally {
                fis.close()
            }
            return keyStore
        } catch (IOException | java.security.cert.CertificateException ex) {
            String message = "TLS keystore password is incorrect"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message, ex)
        }
    }

    private String validateKeyAlias(KeyStore keyStore, EdgeConfig edgeConfig) {
        String configuredAlias = edgeConfig.wss.alias

        if (configuredAlias != null && !configuredAlias.trim().isEmpty()) {
            if (!keyStore.containsAlias(configuredAlias)) {
                List<String> availableAliases = Collections.list(keyStore.aliases())
                String message = "TLS keystore alias '${configuredAlias}' not found. Available aliases: ${availableAliases}"
                logger.error("CRITICAL: {}", message)
                throw new TlsValidationException(message)
            }
            return configuredAlias
        }

        // No alias configured — find the first PrivateKeyEntry
        Enumeration<String> aliases = keyStore.aliases()
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement()
            if (keyStore.isKeyEntry(alias)) {
                logger.info("No alias configured — using first key entry: '{}'", alias)
                return alias
            }
        }

        String message = "TLS keystore is empty — no key entries found"
        logger.error("CRITICAL: {}", message)
        throw new TlsValidationException(message)
    }

    private void validateCertificateExpiry(KeyStore keyStore, String alias) {
        java.security.cert.Certificate cert = keyStore.getCertificate(alias)

        if (cert == null) {
            String message = "TLS keystore has no certificate for alias '${alias}'"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }

        if (cert instanceof X509Certificate) {
            X509Certificate x509 = (X509Certificate) cert
            Date now = new Date()

            if (x509.notAfter.before(now)) {
                String subjectCN = x509.subjectX500Principal.name
                // Extract CN from the DN
                String cn = subjectCN
                def matcher = (subjectCN =~ /CN=([^,]+)/)
                if (matcher.find()) {
                    cn = matcher.group(1)
                }
                String message = "TLS certificate expired — CN=${cn}, notAfter=${x509.notAfter}"
                logger.error("CRITICAL: {}", message)
                throw new TlsValidationException(message)
            }

            logger.info("TLS certificate valid — CN={}, expires={}",
                cert.subjectX500Principal.name, x509.notAfter)
        }
    }

    /**
     * Validates the mTLS truststore at startup when mTLS is enabled.
     * Checks: path configured, file exists, password correct, at least 1 CA cert entry.
     * Fail-fast with TlsValidationException on any failure.
     *
     * @since US-CERT106-01 (TC-70, TC-69)
     */
    private void validateTruststore(EdgeConfig edgeConfig) {
        String truststorePath = edgeConfig.mtls.truststorePath

        // Check truststore path is configured
        if (truststorePath == null || truststorePath.trim().isEmpty()) {
            String message = "mTLS truststore path is not configured (TRUSTSTORE_PATH)"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }

        // Check truststore file exists
        File truststoreFile = new File(truststorePath)
        if (!truststoreFile.exists()) {
            String message = "mTLS truststore file not found: ${truststorePath}"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }

        // Load truststore with password
        KeyStore trustStore
        try {
            trustStore = KeyStore.getInstance("PKCS12")
            FileInputStream fis = new FileInputStream(truststoreFile)
            try {
                trustStore.load(fis, edgeConfig.mtls.truststorePassword?.toCharArray())
            } finally {
                fis.close()
            }
        } catch (IOException | java.security.cert.CertificateException ex) {
            String message = "mTLS truststore password is incorrect: ${truststorePath}"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message, ex)
        }

        // Verify at least one trusted CA certificate entry exists
        int certCount = 0
        Enumeration<String> aliases = trustStore.aliases()
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement()
            if (trustStore.isCertificateEntry(alias)) {
                certCount++
            }
        }

        if (certCount == 0) {
            String message = "mTLS truststore contains no trusted CA certificates: ${truststorePath}"
            logger.error("CRITICAL: {}", message)
            throw new TlsValidationException(message)
        }

        logger.info("mTLS truststore loaded — {} trusted CA certificate(s) from {}", certCount, truststorePath)
    }
}

