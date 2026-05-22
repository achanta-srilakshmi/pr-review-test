package se.controller

import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Produces
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.scheduler.CacheRefreshScheduler
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService

import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * REST endpoint for manual cache refresh operations.
 * Reserved for ops/debugging use only — not intended for direct CSMS service calls.
 *
 * @since US-SE-CACHE108-02
 */
@Controller("/api/v1/cache")
class CacheController {

    private static final Logger logger = LoggerFactory.getLogger(CacheController.class)

    private final CacheRefreshScheduler cacheRefreshScheduler
    private final ChargerIdentifierCacheService chargerIdentifierCacheService
    private final IdTagCacheService idTagCacheService

    CacheController(CacheRefreshScheduler cacheRefreshScheduler,
                    ChargerIdentifierCacheService chargerIdentifierCacheService,
                    IdTagCacheService idTagCacheService) {
        this.cacheRefreshScheduler = cacheRefreshScheduler
        this.chargerIdentifierCacheService = chargerIdentifierCacheService
        this.idTagCacheService = idTagCacheService
    }

    /**
     * Triggers an immediate synchronous refresh of all caches from admin-service.
     * Operates on this single SE instance only.
     *
     * @return JSON with current cache sizes and timestamp
     */
    @Post("/refresh")
    @Produces(MediaType.APPLICATION_JSON)
    Map<String, Object> refresh() {
        logger.info("Manual cache refresh triggered via REST")
        cacheRefreshScheduler.refreshCaches()
        String timestamp = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        return [
                chargerIdentifiersSize: chargerIdentifierCacheService.size(),
                idTagsSize            : idTagCacheService.size(),
                timestamp             : timestamp
        ]
    }
}
