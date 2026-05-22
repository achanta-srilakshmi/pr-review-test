package se.controller

import se.scheduler.CacheRefreshScheduler
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for CacheController.
 * Covers: TC-02-014, TC-03-011
 */
class CacheControllerSpec extends Specification {

    CacheRefreshScheduler cacheRefreshScheduler = Mock()
    ChargerIdentifierCacheService chargerIdentifierCacheService = Mock()
    IdTagCacheService idTagCacheService = Mock()

    @Subject
    CacheController controller = new CacheController(cacheRefreshScheduler, chargerIdentifierCacheService, idTagCacheService)

    // -----------------------------------------------------------------------
    // TC-02-014: POST /api/v1/cache/refresh triggers refreshCaches and returns correct response
    // -----------------------------------------------------------------------
    def "TC-02-014: POST /refresh triggers cache refresh and returns chargerIdentifiersSize, idTagsSize, and timestamp"() {
        given: "cache contains 2 charger entries and 5 idTag entries after refresh"
        chargerIdentifierCacheService.size() >> 2
        idTagCacheService.size() >> 5

        when: "refresh endpoint is called"
        Map<String, Object> response = controller.refresh()

        then: "refreshCaches is called exactly once"
        1 * cacheRefreshScheduler.refreshCaches()

        and: "response contains chargerIdentifiersSize"
        response.chargerIdentifiersSize == 2

        and: "response contains idTagsSize"
        response.idTagsSize == 5

        and: "response contains a non-null ISO-8601 timestamp"
        response.timestamp != null
        response.timestamp instanceof String
        response.timestamp ==~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*/
    }

    // -----------------------------------------------------------------------
    // TC-03-011: Manual refresh response includes both cache sizes
    // -----------------------------------------------------------------------
    def "TC-03-011: refresh response includes both chargerIdentifiersSize and idTagsSize fields"() {
        given: "charger cache has 10 entries, idTag cache has 50 entries"
        chargerIdentifierCacheService.size() >> 10
        idTagCacheService.size() >> 50

        when:
        Map<String, Object> response = controller.refresh()

        then:
        1 * cacheRefreshScheduler.refreshCaches()
        response.chargerIdentifiersSize == 10
        response.idTagsSize == 50
        response.timestamp != null
    }

    // -----------------------------------------------------------------------
    // Additional: response timestamp is in UTC (ends with Z or +00:00)
    // -----------------------------------------------------------------------
    def "refresh response timestamp is UTC"() {
        given:
        chargerIdentifierCacheService.size() >> 0
        idTagCacheService.size() >> 0

        when:
        Map<String, Object> response = controller.refresh()

        then:
        1 * cacheRefreshScheduler.refreshCaches()
        (response.timestamp as String).endsWith("Z") || (response.timestamp as String).contains("+00:00")
    }

    // -----------------------------------------------------------------------
    // Additional: refresh returns 0 when both caches are empty after refresh
    // -----------------------------------------------------------------------
    def "refresh returns 0 for both sizes when caches are empty"() {
        given:
        chargerIdentifierCacheService.size() >> 0
        idTagCacheService.size() >> 0

        when:
        Map<String, Object> response = controller.refresh()

        then:
        1 * cacheRefreshScheduler.refreshCaches()
        response.chargerIdentifiersSize == 0
        response.idTagsSize == 0
    }
}
