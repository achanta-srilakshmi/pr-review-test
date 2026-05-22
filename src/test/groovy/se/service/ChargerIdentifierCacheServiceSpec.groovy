package se.service

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for ChargerIdentifierCacheService.
 * Covers: TC-02-008, TC-02-009, TC-02-012, TC-02-015
 */
class ChargerIdentifierCacheServiceSpec extends Specification {

    @Subject
    ChargerIdentifierCacheService service = new ChargerIdentifierCacheService(new SimpleMeterRegistry())

    // -----------------------------------------------------------------------
    // TC-02-008: Sanity check — refuses empty swap when current cache > 100 entries
    // -----------------------------------------------------------------------
    def "TC-02-008: refuses empty list swap when current cache exceeds 100 entries"() {
        given: "cache preloaded with 150 entries"
        def initial = (1..150).collect { "CP-" + it }   // concatenation produces String, not GString
        service.refreshCache(initial)
        assert service.size() == 150

        when: "admin-service returns an empty list"
        service.refreshCache([])

        then: "swap is refused — existing 150 entries retained"
        service.size() == 150
        service.isAuthorized("CP-1") == true
        service.isCacheLoaded() == true
    }

    // -----------------------------------------------------------------------
    // TC-02-009: Sanity check — accepts empty swap when current cache <= 100 entries
    // -----------------------------------------------------------------------
    def "TC-02-009: accepts empty list swap when current cache is at or below 100 entries"() {
        given: "cache preloaded with 5 entries"
        service.refreshCache(["CP-A", "CP-B", "CP-C", "CP-D", "CP-E"])
        assert service.size() == 5

        when: "admin-service returns an empty list"
        service.refreshCache([])

        then: "swap proceeds — cache becomes empty"
        service.size() == 0
        service.isAuthorized("CP-A") == false
        service.isCacheLoaded() == true
    }

    // -----------------------------------------------------------------------
    // TC-02-009b: First-load with empty list (cacheLoaded=false, size=0) proceeds
    // -----------------------------------------------------------------------
    def "TC-02-009b: first load with empty list is accepted (initial size is 0)"() {
        given: "fresh service — cache is empty and not loaded"
        assert service.size() == 0
        assert !service.isCacheLoaded()

        when: "admin-service returns an empty list on first tick"
        service.refreshCache([])

        then: "swap proceeds; cacheLoaded becomes true"
        service.isCacheLoaded() == true
        service.size() == 0
    }

    // -----------------------------------------------------------------------
    // TC-02-012: Atomic swap replaces entire set — old identifiers removed, new ones added
    // -----------------------------------------------------------------------
    def "TC-02-012: atomic swap replaces old set with new set"() {
        given: "cache loaded with CP-001 and CP-002"
        service.refreshCache(["CP-001", "CP-002"])
        assert service.isAuthorized("CP-001")

        when: "cache refreshed with CP-003 and CP-004 only"
        service.refreshCache(["CP-003", "CP-004"])

        then: "old identifiers are gone; new ones are present"
        service.isAuthorized("CP-001") == false
        service.isAuthorized("CP-002") == false
        service.isAuthorized("CP-003") == true
        service.isAuthorized("CP-004") == true
        service.size() == 2
    }

    // -----------------------------------------------------------------------
    // TC-02-015: Gauge updated after each successful swap
    // -----------------------------------------------------------------------
    def "TC-02-015: size gauge reflects latest cache size after each swap"() {
        given: "a service with a real SimpleMeterRegistry"
        def registry = new SimpleMeterRegistry()
        def svc = new ChargerIdentifierCacheService(registry)

        when: "first load of 50 entries"
        svc.refreshCache((1..50).collect { "CP-" + it })

        then: "gauge reports 50"
        svc.size() == 50

        when: "second refresh returns 75 entries"
        svc.refreshCache((1..75).collect { "CP-" + it })

        then: "gauge reports 75"
        svc.size() == 75
    }

    // -----------------------------------------------------------------------
    // Additional: null incoming list treated as empty (no NPE)
    // -----------------------------------------------------------------------
    def "null incoming list is treated as empty list — no NullPointerException"() {
        given: "fresh service"

        when: "null is passed as incoming list"
        service.refreshCache(null)

        then: "no exception; cacheLoaded becomes true"
        noExceptionThrown()
        service.isCacheLoaded() == true
        service.size() == 0
    }

    // -----------------------------------------------------------------------
    // Additional: isAuthorized is case-sensitive (exact match)
    // -----------------------------------------------------------------------
    def "isAuthorized is case-sensitive"() {
        given: "cache contains 'CP-001'"
        service.refreshCache(["CP-001"])

        expect:
        service.isAuthorized("CP-001") == true
        service.isAuthorized("cp-001") == false
        service.isAuthorized("CP-001 ") == false
    }
}
