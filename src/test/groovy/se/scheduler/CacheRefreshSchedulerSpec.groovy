package se.scheduler

import se.client.AdminCacheClient
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for CacheRefreshScheduler.
 * Covers: TC-02-010, TC-02-013, TC-03-010
 */
class CacheRefreshSchedulerSpec extends Specification {

    AdminCacheClient adminCacheClient = Mock()
    ChargerIdentifierCacheService chargerIdentifierCacheService = Mock()
    IdTagCacheService idTagCacheService = Mock()

    @Subject
    CacheRefreshScheduler scheduler = new CacheRefreshScheduler(adminCacheClient, chargerIdentifierCacheService, idTagCacheService)

    // -----------------------------------------------------------------------
    // TC-02-010: Admin service down — null returned — existing cache retained, scheduler survives
    // -----------------------------------------------------------------------
    def "TC-02-010: admin service failure returns null — no refreshCache call — no exception propagated"() {
        given: "admin-service throws connection exception for both fetches"
        adminCacheClient.fetchChargerIdentifiers() >> null
        adminCacheClient.fetchIdTags() >> null

        when: "scheduler tick fires"
        scheduler.refreshCaches()

        then: "refreshCache is NOT called on either service — existing caches retained"
        0 * chargerIdentifierCacheService.refreshCache(_)
        0 * idTagCacheService.refreshCache(_)

        and: "no exception propagated — scheduler keeps running"
        noExceptionThrown()
    }

    // -----------------------------------------------------------------------
    // TC-02-010b: Successful fetch delegates to refreshCache
    // -----------------------------------------------------------------------
    def "TC-02-010b: successful admin-service fetch delegates to refreshCache"() {
        given: "admin-service returns 3 identifiers"
        adminCacheClient.fetchChargerIdentifiers() >> ["CP-001", "CP-002", "CP-003"]
        adminCacheClient.fetchIdTags() >> []

        when: "scheduler fires refreshCaches()"
        scheduler.refreshCaches()

        then: "refreshCache is called with the returned list"
        1 * chargerIdentifierCacheService.refreshCache(["CP-001", "CP-002", "CP-003"])
    }

    // -----------------------------------------------------------------------
    // TC-02-013: No active eviction — scheduler only calls refreshCache; never calls closeSession
    // -----------------------------------------------------------------------
    def "TC-02-013: scheduler does not call closeSession on connected chargers during refresh"() {
        given: "admin-service returns new list without CP-OLD"
        adminCacheClient.fetchChargerIdentifiers() >> ["CP-NEW"]
        adminCacheClient.fetchIdTags() >> []

        when: "scheduled refresh fires"
        scheduler.refreshCaches()

        then: "refreshCache is called with the new list"
        1 * chargerIdentifierCacheService.refreshCache(["CP-NEW"])

        and: "no other interactions on chargerIdentifierCacheService — no session closure triggered"
        0 * chargerIdentifierCacheService._
        // ConnectionService is not involved at all — eviction is NOT performed
    }

    // -----------------------------------------------------------------------
    // Additional: empty list from admin-service is passed through to refreshCache (sanity is in service)
    // -----------------------------------------------------------------------
    def "empty list returned by admin-service is passed to refreshCache — sanity handled in service layer"() {
        given:
        adminCacheClient.fetchChargerIdentifiers() >> []
        adminCacheClient.fetchIdTags() >> []

        when:
        scheduler.refreshCaches()

        then: "refreshCache receives the empty list — sanity check is the service's responsibility"
        1 * chargerIdentifierCacheService.refreshCache([])
    }

    // -----------------------------------------------------------------------
    // TC-03-010: Shared scheduler refreshes both charger identifier and idTag caches
    // -----------------------------------------------------------------------
    def "TC-03-010: refreshCaches calls both charger identifier and idTag cache services"() {
        given: "admin-service returns data for both caches"
        adminCacheClient.fetchChargerIdentifiers() >> ["CP-001", "CP-002"]
        adminCacheClient.fetchIdTags() >> ["RFID-001", "RFID-002", "RFID-003"]

        when: "scheduler fires refreshCaches()"
        scheduler.refreshCaches()

        then: "charger identifier cache is refreshed"
        1 * chargerIdentifierCacheService.refreshCache(["CP-001", "CP-002"])

        and: "idTag cache is refreshed"
        1 * idTagCacheService.refreshCache(["RFID-001", "RFID-002", "RFID-003"])
    }

    // -----------------------------------------------------------------------
    // TC-03-010b: idTag fetch failure retains idTag cache, charger cache still refreshed
    // -----------------------------------------------------------------------
    def "TC-03-010b: idTag fetch failure retains idTag cache but does not affect charger identifier refresh"() {
        given: "charger fetch succeeds, idTag fetch fails"
        adminCacheClient.fetchChargerIdentifiers() >> ["CP-001"]
        adminCacheClient.fetchIdTags() >> null

        when:
        scheduler.refreshCaches()

        then: "charger identifier cache is refreshed normally"
        1 * chargerIdentifierCacheService.refreshCache(["CP-001"])

        and: "idTag refreshCache is NOT called — existing cache retained"
        0 * idTagCacheService.refreshCache(_)
    }

    // -----------------------------------------------------------------------
    // Additional: refreshCaches() calls scheduledRefresh-equivalent path without throwing
    // -----------------------------------------------------------------------
    def "scheduledRefresh delegates to refreshCaches without exception"() {
        given:
        adminCacheClient.fetchChargerIdentifiers() >> ["CP-001"]
        adminCacheClient.fetchIdTags() >> []

        when:
        scheduler.scheduledRefresh()

        then:
        1 * chargerIdentifierCacheService.refreshCache(["CP-001"])
        noExceptionThrown()
    }
}
