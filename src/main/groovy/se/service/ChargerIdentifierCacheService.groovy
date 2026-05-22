package se.service

import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * In-memory cache of authorized charger identifiers, maintained by CacheRefreshScheduler.
 *
 * Uses a volatile Set reference for lock-free reads with atomic full-set swap on each refresh.
 * Readers always see a complete set — never a partially-built one.
 *
 * Sanity check: if admin-service returns an empty list and the current cache has > 100 entries,
 * the swap is refused and the existing cache is retained to prevent mass charger disconnection.
 *
 * @since US-SE-CACHE108-02
 */
@Singleton
class ChargerIdentifierCacheService {

    private static final Logger logger = LoggerFactory.getLogger(ChargerIdentifierCacheService.class)
    private static final int SANITY_CHECK_THRESHOLD = 100

    private volatile Set<String> authorizedIdentifiers = ConcurrentHashMap.newKeySet()
    private volatile boolean cacheLoaded = false

    private final AtomicInteger sizeGauge

    ChargerIdentifierCacheService(MeterRegistry meterRegistry) {
        this.sizeGauge = new AtomicInteger(0)
        meterRegistry.gauge("se.cache.charger_identifiers.size", sizeGauge)
    }

    /**
     * Atomically replaces the current authorized identifier set with the incoming list.
     * Refuses the swap if the incoming list is empty and the current cache exceeds the sanity threshold.
     *
     * @param incoming List of charger identifier strings from admin-service bulk API; null treated as empty
     */
    void refreshCache(List<String> incoming) {
        List<String> identifiers = incoming ?: Collections.emptyList()

        if (identifiers.isEmpty() && authorizedIdentifiers.size() > SANITY_CHECK_THRESHOLD) {
            logger.error("Refusing charger ID cache swap: admin-service returned empty list but current cache size={} — retaining existing cache",
                    authorizedIdentifiers.size())
            return
        }

        Set<String> newSet = ConcurrentHashMap.newKeySet()
        newSet.addAll(identifiers)
        authorizedIdentifiers = newSet   // single volatile write — readers see complete new set immediately
        cacheLoaded = true
        sizeGauge.set(newSet.size())

        logger.info("Charger identifier cache refreshed: {} entries", newSet.size())
    }

    /**
     * Returns true if the given charger identifier is present in the authorized set.
     * Lock-free read against the volatile reference.
     *
     * @param identifier charger identifier (from WebSocket path)
     * @return true if authorized
     */
    boolean isAuthorized(String identifier) {
        return authorizedIdentifiers.contains(identifier)
    }

    /**
     * Returns true once the cache has been successfully loaded at least once.
     * Used by CacheReadinessIndicator to gate K8s readiness.
     */
    boolean isCacheLoaded() {
        return cacheLoaded
    }

    /**
     * Returns the current number of entries in the cache.
     */
    int size() {
        return authorizedIdentifiers.size()
    }
}
