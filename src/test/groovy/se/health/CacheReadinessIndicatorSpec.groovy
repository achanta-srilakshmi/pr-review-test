package se.health

import io.micronaut.health.HealthStatus
import io.micronaut.management.health.indicator.HealthResult
import org.reactivestreams.Publisher
import reactor.core.publisher.Mono
import se.EdgeConfig
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for CacheReadinessIndicator.
 * Covers: TC-02-005, TC-02-006, TC-02-007, TC-03-012, TC-03-013
 */
class CacheReadinessIndicatorSpec extends Specification {

    EdgeConfig edgeConfig = new EdgeConfig()
    ChargerIdentifierCacheService cacheService = Stub()
    IdTagCacheService idTagCacheService = Stub()

    @Subject
    CacheReadinessIndicator indicator

    def setup() {
        edgeConfig.cacheValidation = new EdgeConfig.CacheValidationConfig()
        indicator = new CacheReadinessIndicator(edgeConfig, cacheService, idTagCacheService)
    }

    // -----------------------------------------------------------------------
    // TC-02-005: UP when chargerIdCheck=false and cache not loaded
    // -----------------------------------------------------------------------
    def "TC-02-005: returns UP when chargerIdCheck=false regardless of cache load state"() {
        given: "enforcement disabled; cache not yet loaded"
        edgeConfig.cacheValidation.chargerIdCheck = false
        edgeConfig.cacheValidation.idTagCheck = false
        cacheService.isCacheLoaded() >> false
        idTagCacheService.isCacheLoaded() >> false

        when: "readiness probe fires"
        HealthResult result = Mono.from(indicator.getResult()).block()

        then: "status is UP"
        result.status == HealthStatus.UP
    }

    // -----------------------------------------------------------------------
    // TC-02-006: DOWN when chargerIdCheck=true and cache not yet loaded
    // -----------------------------------------------------------------------
    def "TC-02-006: returns DOWN when chargerIdCheck=true and cache not yet loaded"() {
        given: "enforcement active; cache not yet loaded"
        edgeConfig.cacheValidation.chargerIdCheck = true
        edgeConfig.cacheValidation.idTagCheck = false
        cacheService.isCacheLoaded() >> false

        when: "readiness probe fires"
        HealthResult result = Mono.from(indicator.getResult()).block()

        then: "status is DOWN"
        result.status == HealthStatus.DOWN

        and: "details include reason"
        result.details['chargerIdCheck'] == true
        result.details['chargerCacheLoaded'] == false
        result.details.containsKey('reason')
    }

    // -----------------------------------------------------------------------
    // TC-02-007: UP when chargerIdCheck=true and cache is loaded
    // -----------------------------------------------------------------------
    def "TC-02-007: returns UP when chargerIdCheck=true and cache is loaded"() {
        given: "enforcement active; cache successfully loaded with 10 entries"
        edgeConfig.cacheValidation.chargerIdCheck = true
        edgeConfig.cacheValidation.idTagCheck = false
        cacheService.isCacheLoaded() >> true
        cacheService.size() >> 10
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.size() >> 0

        when: "readiness probe fires"
        HealthResult result = Mono.from(indicator.getResult()).block()

        then: "status is UP"
        result.status == HealthStatus.UP

        and: "details include cache size"
        result.details['chargerIdCheck'] == true
        result.details['chargerCacheLoaded'] == true
        result.details['chargerCacheSize'] == 10
    }

    // -----------------------------------------------------------------------
    // TC-03-012: DOWN when idTagCheck=true and idTag cache not yet loaded
    // -----------------------------------------------------------------------
    def "TC-03-012: returns DOWN when idTagCheck=true and idTag cache not yet loaded"() {
        given: "charger cache loaded; idTag enforcement active but idTag cache not loaded"
        edgeConfig.cacheValidation.chargerIdCheck = false
        edgeConfig.cacheValidation.idTagCheck = true
        cacheService.isCacheLoaded() >> true
        idTagCacheService.isCacheLoaded() >> false

        when: "readiness probe fires"
        HealthResult result = Mono.from(indicator.getResult()).block()

        then: "status is DOWN"
        result.status == HealthStatus.DOWN

        and: "details include idTag reason"
        result.details['idTagCheck'] == true
        result.details['idTagCacheLoaded'] == false
        result.details.containsKey('reason')
    }

    // -----------------------------------------------------------------------
    // TC-03-013: UP when idTagCheck=false even if idTag cache not loaded
    // -----------------------------------------------------------------------
    def "TC-03-013: returns UP when idTagCheck=false regardless of idTag cache load state"() {
        given: "both checks disabled; neither cache loaded"
        edgeConfig.cacheValidation.chargerIdCheck = false
        edgeConfig.cacheValidation.idTagCheck = false
        cacheService.isCacheLoaded() >> false
        idTagCacheService.isCacheLoaded() >> false

        when: "readiness probe fires"
        HealthResult result = Mono.from(indicator.getResult()).block()

        then: "status is UP"
        result.status == HealthStatus.UP
    }

    // -----------------------------------------------------------------------
    // Additional: UP when chargerIdCheck=false and cache is loaded (baseline sanity)
    // -----------------------------------------------------------------------
    def "UP when chargerIdCheck=false and cache is loaded"() {
        given:
        edgeConfig.cacheValidation.chargerIdCheck = false
        edgeConfig.cacheValidation.idTagCheck = false
        cacheService.isCacheLoaded() >> true
        cacheService.size() >> 5
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.size() >> 0

        when:
        HealthResult result = Mono.from(indicator.getResult()).block()

        then:
        result.status == HealthStatus.UP
    }
}
