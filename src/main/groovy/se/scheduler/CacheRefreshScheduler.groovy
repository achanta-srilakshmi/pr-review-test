package se.scheduler

import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.client.AdminCacheClient
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService

/**
 * Scheduled task that refreshes the charger identifier cache every 5 minutes.
 * Fires immediately on startup (initialDelay = 0s) to ensure the cache is warm
 * before K8s routes traffic to this pod.
 *
 * Extended in US-SE-CACHE108-03 to also refresh the idTag cache.
 *
 * @since US-SE-CACHE108-02
 */
@Singleton
class CacheRefreshScheduler {

    private static final Logger logger = LoggerFactory.getLogger(CacheRefreshScheduler.class)

    private final AdminCacheClient adminCacheClient
    private final ChargerIdentifierCacheService chargerIdentifierCacheService
    private final IdTagCacheService idTagCacheService

    CacheRefreshScheduler(AdminCacheClient adminCacheClient,
                          ChargerIdentifierCacheService chargerIdentifierCacheService,
                          IdTagCacheService idTagCacheService) {
        this.adminCacheClient = adminCacheClient
        this.chargerIdentifierCacheService = chargerIdentifierCacheService
        this.idTagCacheService = idTagCacheService
    }

    /**
     * Scheduled refresh — fires immediately on startup then every 5 minutes.
     * On admin-service failure the existing cache is retained and the next tick will retry.
     */
    @Scheduled(initialDelay = '0s', fixedDelay = '${se.config.cacheRefresh.fixedDelay:5m}')
    void scheduledRefresh() {
        logger.debug("CacheRefreshScheduler tick fired")
        refreshCaches()
    }

    /**
     * Performs an immediate synchronous refresh of all caches.
     * Exposed as a public method so CacheController can trigger manual refresh.
     */
    void refreshCaches() {
        refreshChargerIdentifiers()
        refreshIdTags()
    }

    private void refreshChargerIdentifiers() {
        List<String> identifiers = adminCacheClient.fetchChargerIdentifiers()
        if (identifiers == null) {
            logger.warn("Charger identifier fetch failed — retaining existing cache (size={})",
                    chargerIdentifierCacheService.size())
            return
        }
        chargerIdentifierCacheService.refreshCache(identifiers)
    }

    private void refreshIdTags() {
        List<String> tags = adminCacheClient.fetchIdTags()
        if (tags == null) {
            logger.warn("idTag fetch failed — retaining existing cache (size={})",
                    idTagCacheService.size())
            return
        }
        idTagCacheService.refreshCache(tags)
    }
}
