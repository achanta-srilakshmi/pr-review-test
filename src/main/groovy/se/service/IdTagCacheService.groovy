package se.service

import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * In-memory cache of valid idTag UIDs, maintained by CacheRefreshScheduler.
 *
 * Mirrors ChargerIdentifierCacheService: volatile reference with atomic full-set swap.
 * Sanity check prevents mass rejection when admin-service returns empty response
 * while cache is large (> 100 entries).
 *
 * Gate logic (in CoreProfile16EH):
 *   - idTagCheck=false (default): gate bypassed entirely, existing flow unchanged
 *   - idTagCheck=true + !cacheLoaded: fail-safe reject (readiness probe should prevent routing)
 *   - idTagCheck=true + cacheLoaded + !isValid: reject with AuthorizationStatus.Invalid
 *
 * @since US-SE-CACHE108-03
 */
@Singleton
class IdTagCacheService {

    private static final Logger logger = LoggerFactory.getLogger(IdTagCacheService.class)
    private static final int SANITY_CHECK_THRESHOLD = 100

    private volatile Set<String> validIdTags = ConcurrentHashMap.newKeySet()
    private volatile boolean cacheLoaded = false

    private final AtomicInteger sizeGauge

    IdTagCacheService(MeterRegistry meterRegistry) {
        this.sizeGauge = new AtomicInteger(0)
        meterRegistry.gauge("se.cache.id_tags.size", sizeGauge)
    }

    /**
     * Atomically replaces the current valid idTag set with the incoming list.
     * Refuses the swap if the incoming list is empty and the current cache exceeds the sanity threshold.
     *
     * @param incoming List of idTag UID strings from admin-service bulk API; null treated as empty
     */
    void refreshCache(List<String> incoming) {
        List<String> tags = incoming ?: Collections.emptyList()

        if (tags.isEmpty() && validIdTags.size() > SANITY_CHECK_THRESHOLD) {
            logger.error("Refusing idTag cache swap: admin-service returned empty list but current cache size={} — retaining existing cache",
                    validIdTags.size())
            return
        }

        Set<String> newSet = ConcurrentHashMap.newKeySet()
        newSet.addAll(tags)
        validIdTags = newSet   // single volatile write — readers see complete new set immediately
        cacheLoaded = true
        sizeGauge.set(newSet.size())

        logger.info("idTag cache refreshed: {} entries", newSet.size())
    }

    /**
     * Returns true if the given idTag UID is present in the valid set.
     * Lock-free read against the volatile reference.
     *
     * @param idTag idTag UID string from the OCPP request
     * @return true if valid
     */
    boolean isValid(String idTag) {
        return validIdTags.contains(idTag)
    }

    /**
     * Returns true once the cache has been successfully loaded at least once.
     * Used by CacheReadinessIndicator and CoreProfile16EH gate.
     */
    boolean isCacheLoaded() {
        return cacheLoaded
    }

    /**
     * Returns the current number of entries in the cache.
     */
    int size() {
        return validIdTags.size()
    }
}
