package se.health

import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthIndicator
import io.micronaut.management.health.indicator.HealthResult
import jakarta.inject.Singleton
import org.reactivestreams.Publisher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import se.EdgeConfig
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService

/**
 * K8s readiness indicator for the charger identifier and idTag caches.
 *
 * Returns DOWN (503) when any active enforcement flag is true AND its cache has not yet loaded.
 * This prevents K8s from routing WebSocket connections to a pod before the first
 * scheduler tick (initialDelay=0s) has completed the relevant cache load.
 *
 * When both chargerIdCheck=false and idTagCheck=false (default), returns UP immediately
 * regardless of cache state — zero impact on existing deployments.
 *
 * @since US-SE-CACHE108-02
 * @extended US-SE-CACHE108-03 — added idTag cache readiness check
 */
@Singleton
class CacheReadinessIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(CacheReadinessIndicator.class)

    private final EdgeConfig edgeConfig
    private final ChargerIdentifierCacheService chargerIdentifierCacheService
    private final IdTagCacheService idTagCacheService

    CacheReadinessIndicator(EdgeConfig edgeConfig,
                            ChargerIdentifierCacheService chargerIdentifierCacheService,
                            IdTagCacheService idTagCacheService) {
        this.edgeConfig = edgeConfig
        this.chargerIdentifierCacheService = chargerIdentifierCacheService
        this.idTagCacheService = idTagCacheService
    }

    @Override
    Publisher<HealthResult> getResult() {
        HealthResult result = buildResult()
        return Mono.just(result)
    }

    private HealthResult buildResult() {
        boolean chargerIdCheck = edgeConfig.cacheValidation.chargerIdCheck
        boolean chargerCacheLoaded = chargerIdentifierCacheService.isCacheLoaded()
        boolean idTagCheck = edgeConfig.cacheValidation.idTagCheck
        boolean idTagCacheLoaded = idTagCacheService.isCacheLoaded()

        if (chargerIdCheck && !chargerCacheLoaded) {
            logger.warn("Readiness DOWN: chargerIdCheck=true but charger identifier cache not yet loaded")
            return HealthResult.builder("cache-readiness", HealthStatus.DOWN)
                    .details([
                            chargerIdCheck    : chargerIdCheck,
                            chargerCacheLoaded: chargerCacheLoaded,
                            idTagCheck        : idTagCheck,
                            idTagCacheLoaded  : idTagCacheLoaded,
                            reason            : "Charger identifier cache not loaded — enforcement is active"
                    ])
                    .build()
        }

        if (idTagCheck && !idTagCacheLoaded) {
            logger.warn("Readiness DOWN: idTagCheck=true but idTag cache not yet loaded")
            return HealthResult.builder("cache-readiness", HealthStatus.DOWN)
                    .details([
                            chargerIdCheck    : chargerIdCheck,
                            chargerCacheLoaded: chargerCacheLoaded,
                            idTagCheck        : idTagCheck,
                            idTagCacheLoaded  : idTagCacheLoaded,
                            reason            : "idTag cache not loaded — enforcement is active"
                    ])
                    .build()
        }

        return HealthResult.builder("cache-readiness", HealthStatus.UP)
                .details([
                        chargerIdCheck    : chargerIdCheck,
                        chargerCacheLoaded: chargerCacheLoaded,
                        chargerCacheSize  : chargerIdentifierCacheService.size(),
                        idTagCheck        : idTagCheck,
                        idTagCacheLoaded  : idTagCacheLoaded,
                        idTagCacheSize    : idTagCacheService.size()
                ])
                .build()
    }
}
