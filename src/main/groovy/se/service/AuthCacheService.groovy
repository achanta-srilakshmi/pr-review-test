package se.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.cache.annotation.CacheInvalidate
import io.micronaut.cache.annotation.Cacheable
import io.micronaut.cache.annotation.CachePut
import io.micronaut.http.client.HttpClient
import io.micronaut.http.uri.UriBuilder
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig

@Singleton
class AuthCacheService {

  @Inject
  EdgeConfig edgeConfig

  @Inject
  ObjectMapper objectMapper

  @Inject
  UtilService utilService

  Logger logger = LoggerFactory.getLogger(AuthCacheService.class)

  private final Map<String, ChargerCredentials> chargerCache = [:]

  AuthCacheService() {

  }

  /**
   * Determines if a charger needs to be authenticated based on its identifier.
   *
   * @param identifier The identifier of the charger.
   * @return true if the charger should be authenticated, false otherwise.
   */
  boolean shouldAuthenticate(String identifier) {
    boolean restriction = !edgeConfig?.authorizedSocket?.allowAnonymous
    try {
      logger.trace("Checking if {} needs to be authorized before allowing to connect", identifier)

      if(restriction)
        return restriction

      ChargerCredentials cachedValue = this.getChargerCredentials().get(identifier)

      if(cachedValue == null) return restriction

      logger.trace("shouldAuthenticate flag for {} is {}", identifier, cachedValue.authFlg)
      if (cachedValue != null) {
        return cachedValue.getAuthFlg()
      } else {
        return restriction
      }
    } catch (Exception e) {
      logger.error("Error in determining the authorization access for {}", identifier, e)
      return false //If logic is failing for any reason, it lets the charger connect
    }
  }



  /**
   * Fetches the charger credentials from the admin server.
   *
   * @return A map of charger credentials.
   */
  List<ChargerCredentials> fetchChargerCredentials() {
    HttpClient adminClient = null
    try {
      logger.info("Fetching charger credentials")

      adminClient = HttpClient.create(edgeConfig?.adminServerUrl?.toURL())
      UriBuilder uri = UriBuilder.of(edgeConfig?.chargerCredsUri)

      String response = utilService.getClientResponse(adminClient, uri)
      List<ChargerCredentials> chargerCredentials = objectMapper?.readValue(response, Map.class)?.get("data")
      logger.debug("Size of credentials list is : {}", chargerCredentials?.size())

      return chargerCredentials
    } catch (Exception e) {
      logger.error("Error fetching charger credentials", e)
      return null
    } finally {
      adminClient?.close()
    }
  }

  @Cacheable("CredentialCache")
  Map<String, ChargerCredentials> getChargerCredentials() {
    List<ChargerCredentials> chargerCredentials = this.fetchChargerCredentials()
    return chargerCredentials.collectEntries { [(it.chargerId): it] }
  }

  @CachePut("CredentialCache")
  @Scheduled(fixedRate = '1h', initialDelay = '90s')
  Map<String, ChargerCredentials> updateChargerCredentials(List<ChargerCredentials> credentials) {
    return credentials.collectEntries { [(it.chargerId): it] }
  }

  @CacheInvalidate("CredentialCache")
  void invalidateCache() {
    logger.info("Invalidating CredentialCache")
  }
}

class ChargerCredentials {
  String chargerId
  String password
  boolean authFlg
}
