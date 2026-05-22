package se.service

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for IdTagCacheService.
 * Covers: TC-03-009 and basic operation cases.
 */
class IdTagCacheServiceSpec extends Specification {

    @Subject
    IdTagCacheService service = new IdTagCacheService(new SimpleMeterRegistry())

    // -----------------------------------------------------------------------
    // TC-03-009: Sanity check — refuses empty swap when current cache > 100 entries
    // -----------------------------------------------------------------------
    def "TC-03-009: refuses empty list swap when current cache exceeds 100 entries"() {
        given: "cache preloaded with 150 idTag entries"
        def initial = (1..150).collect { "RFID-" + it }
        service.refreshCache(initial)
        assert service.size() == 150

        when: "admin-service returns an empty list"
        service.refreshCache([])

        then: "swap is refused — existing 150 entries retained"
        service.size() == 150
        service.isValid("RFID-1") == true
        service.isCacheLoaded() == true
    }

    // -----------------------------------------------------------------------
    // Sanity check — accepts empty swap when current cache <= 100 entries
    // -----------------------------------------------------------------------
    def "accepts empty list swap when current cache is at or below 100 entries"() {
        given: "cache preloaded with 5 idTag entries"
        service.refreshCache(["TAG-A", "TAG-B", "TAG-C", "TAG-D", "TAG-E"])
        assert service.size() == 5

        when: "admin-service returns an empty list"
        service.refreshCache([])

        then: "swap proceeds — cache becomes empty"
        service.size() == 0
        service.isValid("TAG-A") == false
        service.isCacheLoaded() == true
    }

    // -----------------------------------------------------------------------
    // First load with empty list proceeds (cacheLoaded=false, size=0)
    // -----------------------------------------------------------------------
    def "first load with empty list is accepted — initial size is 0"() {
        given: "fresh service — cache is empty and not loaded"
        assert service.size() == 0
        assert !service.isCacheLoaded()

        when: "admin-service returns an empty list on first tick"
        service.refreshCache([])

        then: "cache is marked loaded with 0 entries"
        service.isCacheLoaded() == true
        service.size() == 0
    }

    // -----------------------------------------------------------------------
    // Null incoming list treated as empty
    // -----------------------------------------------------------------------
    def "null incoming list is treated as empty list — swap proceeds when cache is small"() {
        given: "cache has 2 entries"
        service.refreshCache(["TAG-1", "TAG-2"])

        when: "null is passed"
        service.refreshCache(null)

        then: "cache becomes empty — null treated as empty list"
        service.size() == 0
        service.isCacheLoaded() == true
    }

    // -----------------------------------------------------------------------
    // isValid returns true for present idTag
    // -----------------------------------------------------------------------
    def "isValid returns true for an idTag present in the cache"() {
        given:
        service.refreshCache(["RFID-VALID", "RFID-OTHER"])

        expect:
        service.isValid("RFID-VALID") == true
    }

    // -----------------------------------------------------------------------
    // isValid returns false for absent idTag
    // -----------------------------------------------------------------------
    def "isValid returns false for an idTag not present in the cache"() {
        given:
        service.refreshCache(["RFID-VALID"])

        expect:
        service.isValid("RFID-MISSING") == false
    }

    // -----------------------------------------------------------------------
    // isCacheLoaded starts false, becomes true after first refresh
    // -----------------------------------------------------------------------
    def "isCacheLoaded is false before first refresh and true after"() {
        expect: "initially not loaded"
        !service.isCacheLoaded()

        when:
        service.refreshCache(["RFID-001"])

        then:
        service.isCacheLoaded() == true
    }
}
