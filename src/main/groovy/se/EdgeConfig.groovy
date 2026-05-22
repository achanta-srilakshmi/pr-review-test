package se

import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Property
import io.micronaut.core.convert.format.MapFormat


@ConfigurationProperties('se.config')
class EdgeConfig {

    String handler
    String scheduler
    String kafkaNodeId
    int webSocketPort
    String authServerUrl
    boolean authBypass = false
    boolean logPassword = false
    String dateFormatString = "yyyy-MM-dd'T'HH:mm:ssZ"

    String authUri = "/dm/v4.0/chargeSessions/commands/authorize"
    String adminServerUrl
    String authInfoUri = "/osm/v4.0/chargers/authInfo"
    String chargerCredsUri = "/osm/v4.0/chargers/credentials"

    WSSConfig wss = new WSSConfig()
    Chargers chargers = new Chargers()
    AuthorizedSocket authorizedSocket = new AuthorizedSocket()
    SubprotocolConfig subprotocol = new SubprotocolConfig()
    MtlsConfig mtls = new MtlsConfig()
    String connectionProfile

    @ConfigurationProperties('wss')
    static class WSSConfig {
        boolean enabled = false
        String storeType
        String keyStore
        String storePassword
        String keyPassword
        String algorithm
        String encryptionType
        String alias
        int wssPort = 9943
    }

    @ConfigurationProperties('chargers')
    static class Chargers {
        int heartbeatIntervalInSec = 180
        String statusUpdateFrequency = "5m"
        boolean updateOnNewConnection = true
        String diagnosticLogLocation
    }

    @ConfigurationProperties('authorizedSocket')
    static class AuthorizedSocket {
        boolean enabled = false
        Map credentials
        int cacheMaxAge = 360
        boolean allowAnonymous = true
    }

    /**
     * WebSocket subprotocol negotiation configuration.
     * Controls whether Sec-WebSocket-Protocol header validation is strict or permissive.
     * @since US-SEC104-01 (TC-67)
     */
    @ConfigurationProperties('subprotocol')
    static class SubprotocolConfig {
        /**
         * When true, reject connections missing Sec-WebSocket-Protocol header.
         * When false (default), accept connections without the header for backward
         * compatibility with older charge points.
         */
        boolean strict = false
    }

    /**
     * Mutual TLS (mTLS) configuration for Security Profile 3.
     * Controls truststore loading and client certificate authentication.
     * @since US-CERT106-01 (TC-70, TC-69)
     */
    @ConfigurationProperties('mtls')
    static class MtlsConfig {
        /** Enable mTLS — truststore loading and client cert validation on WSS port */
        boolean enabled = false
        /** Client cert auth mode: OPTIONAL (Profile 2+3 coexist) or REQUIRE (strict Profile 3) */
        String clientAuth = "OPTIONAL"
        /** Path to PKCS12 truststore containing CA certs for CP client cert validation */
        String truststorePath
        /** Truststore password */
        String truststorePassword
        /** Grace period (seconds) to suppress LostSession after SecurityProfile upgrade @since US-CERT106-03 (TC-64) */
        int upgradeGracePeriodSeconds = 60
    }

    /**
     * Cache validation gate configuration.
     * Controls whether charger identifier and idTag caches are enforced at connection/transaction time.
     * Both flags default to false — deploy with enforcement off, enable via K8s ConfigMap without redeployment.
     * @since US-SE-CACHE108-02
     */
    @ConfigurationProperties('cacheValidation')
    static class CacheValidationConfig {
        /**
         * When true, charger WebSocket connections are rejected (close 1008) if the charger
         * identifier is not present in the authorized charger cache.
         * When false (default), the cache gate is skipped entirely — zero impact on existing flow.
         * Env var: SE_CHARGER_ID_CHECK
         */
        boolean chargerIdCheck = false

        /**
         * When true, StartTransaction and Authorize requests are rejected with Invalid
         * if the idTag is not present in the idTag cache.
         * When false (default), the cache gate is skipped — existing DM Server flow unchanged.
         * Env var: SE_ID_TAG_CHECK
         * @since US-SE-CACHE108-03
         */
        boolean idTagCheck = false
    }

    CacheValidationConfig cacheValidation = new CacheValidationConfig()

    /**
     * Cache refresh scheduler configuration.
     * Controls the interval at which CacheRefreshScheduler polls admin-service.
     * @since US-SE-CACHE108-03
     */
    @ConfigurationProperties('cacheRefresh')
    static class CacheRefreshConfig {
        /**
         * How often to poll admin-service for fresh charger identifier and idTag data.
         * Accepts Micronaut duration strings: '5m', '30s', '1h' etc.
         * Defaults to 5 minutes. Env var: SE_CACHE_REFRESH_INTERVAL
         */
        String fixedDelay = '5m'
    }

    CacheRefreshConfig cacheRefresh = new CacheRefreshConfig()

}