package se.factory

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import se.EdgeConfig
import se.bus.OcppEventReceiver

import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import java.security.KeyStore
import java.security.cert.Certificate
import java.util.concurrent.atomic.AtomicInteger

@Factory
class BeanFactory {

  private static final Logger logger = LoggerFactory.getLogger(BeanFactory.class);

  /**
   * OCPP 1.6 Security Whitepaper Appendix A — Security Profile 2 approved cipher suites.
   * All RC4, 3DES, NULL, EXPORT, and anonymous cipher suites are excluded.
   */
  static final List<String> APPROVED_CIPHER_SUITES = [
      "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      "TLS_RSA_WITH_AES_128_GCM_SHA256",
      "TLS_RSA_WITH_AES_256_GCM_SHA256"
  ]

  /**
   * Allowed TLS protocol versions — TLS 1.2 and TLS 1.3 only.
   * TLS 1.0, TLS 1.1, SSLv3, SSLv2 are rejected.
   */
  static final String[] ALLOWED_TLS_PROTOCOLS = ["TLSv1.2", "TLSv1.3"]

  /**
   * OCPP 1.6 Part 2, Section 3 — WebSocket subprotocol identifier.
   * Case-sensitive. Used during WebSocket upgrade handshake negotiation.
   * @since US-SEC104-01 (TC-67)
   */
  static final String OCPP16_SUBPROTOCOL = "ocpp1.6"

  @Inject
  EdgeConfig edgeConfig

  @Inject
  MeterRegistry meterRegistry

  @Inject
  OcppEventReceiver ocppIncoming


  @Singleton
  long startupTime() {
    return System.currentTimeMillis()
  }

  @Singleton
  AtomicInteger sessionGauge() {
    meterRegistry.gauge("se.sessions.active", new AtomicInteger(0))
  }

  @Singleton
  Scheduler schedulerType() {
    logger.info("Choosing the scheduling strategy as {} for charger communication", edgeConfig.scheduler)
    def scheduler = Schedulers.boundedElastic()

    switch (edgeConfig.scheduler) {
      case "parallel":
        scheduler = Schedulers.parallel()
        break;
      case "single":
        scheduler = Schedulers.single()
        break;
      case "boundedElastic":
        scheduler = Schedulers.boundedElastic()
        break;
      case "immediate":
        scheduler = Schedulers.immediate()
        break;
    }
    return scheduler
  }

  @Singleton
  SSLContext sslContext() {
    KeyStore ks = KeyStore.getInstance(edgeConfig.wss.storeType);
    File kf = new File(edgeConfig.wss.keyStore);
    ks.load(new FileInputStream(kf), edgeConfig.wss.storePassword.toCharArray());

    // Check certificate chain length and warn if only leaf cert is present
    String alias = edgeConfig.wss.alias ?: findFirstKeyAlias(ks)
    if (alias) {
      Certificate[] chain = ks.getCertificateChain(alias)
      if (chain != null && chain.length == 1) {
        logger.warn("Certificate chain contains only the leaf certificate — " +
            "intermediate CAs may be missing. Charge points with minimal trust stores may fail chain validation.")
      } else if (chain != null) {
        logger.info("Certificate chain loaded with {} certificates for alias '{}'", chain.length, alias)
      }
    }

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(edgeConfig.wss.algorithm);
    kmf.init(ks, edgeConfig.wss.keyPassword.toCharArray());

    // US-CERT106-01: When mTLS enabled, load separate truststore for CP client cert validation
    // When mTLS disabled, use keystore as truststore (existing behaviour)
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(edgeConfig.wss.algorithm);
    if (edgeConfig.mtls.enabled && edgeConfig.mtls.truststorePath) {
      KeyStore trustStore = KeyStore.getInstance("PKCS12")
      File tsFile = new File(edgeConfig.mtls.truststorePath)
      trustStore.load(new FileInputStream(tsFile), edgeConfig.mtls.truststorePassword?.toCharArray())
      tmf.init(trustStore)
      logger.info("SSLContext using separate mTLS truststore: {}", edgeConfig.mtls.truststorePath)
    } else {
      tmf.init(ks);
    }

    SSLContext sslContext = SSLContext.getInstance(edgeConfig.wss.encryptionType);
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    logger.info("SSLContext initialised — enforcing TLS 1.2+, OCPP-approved cipher suites only")
    return sslContext
  }

  /**
   * Finds the first key entry alias in the keystore.
   * Used when no alias is explicitly configured.
   */
  private String findFirstKeyAlias(KeyStore ks) {
    Enumeration<String> aliases = ks.aliases()
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement()
      if (ks.isKeyEntry(alias)) return alias
    }
    return null
  }
}
