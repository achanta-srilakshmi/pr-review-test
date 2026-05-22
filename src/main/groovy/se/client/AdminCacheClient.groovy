package se.client

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.http.HttpRequest
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.DefaultHttpClientConfiguration
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig

import java.time.Duration

/**
 * HTTP client for fetching bulk cache data from admin-service.
 * Called by CacheRefreshScheduler every 5 minutes and on startup.
 * All calls use a 10-second read timeout to prevent blocking the scheduler thread.
 *
 * @since US-SE-CACHE108-02
 */
@Singleton
class AdminCacheClient {

    private static final Logger logger = LoggerFactory.getLogger(AdminCacheClient.class)

    private static final String CHARGER_IDENTIFIERS_URI = "/osm/v4.0/cache/charger-identifiers"
    private static final String ID_TAGS_URI = "/osm/v4.0/cache/id-tags"
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10)

    private final EdgeConfig edgeConfig
    private final ObjectMapper objectMapper

    AdminCacheClient(EdgeConfig edgeConfig, ObjectMapper objectMapper) {
        this.edgeConfig = edgeConfig
        this.objectMapper = objectMapper
    }

    /**
     * Fetches all authorized charger identifiers from admin-service bulk API.
     * Returns a flat list of charger identifier strings.
     * Returns null on any error (connection failure, timeout, non-2xx response) — caller retains existing cache.
     *
     * @return List of charger identifier strings, or null on failure
     */
    List<String> fetchChargerIdentifiers() {
        return fetchStringList(CHARGER_IDENTIFIERS_URI, "charger identifiers")
    }

    /**
     * Fetches all valid idTag UIDs from admin-service bulk API.
     * Returns a flat list of idTag UID strings.
     * Returns null on any error — caller retains existing cache.
     *
     * @return List of idTag UID strings, or null on failure
     * @since US-SE-CACHE108-03
     */
    List<String> fetchIdTags() {
        return fetchStringList(ID_TAGS_URI, "idTags")
    }

    private List<String> fetchStringList(String uri, String description) {
        HttpClient client = null
        try {
            logger.debug("Fetching {} from admin-service: {}", description, uri)

            DefaultHttpClientConfiguration config = new DefaultHttpClientConfiguration()
            config.setReadTimeout(HTTP_TIMEOUT)

            client = HttpClient.create(edgeConfig.adminServerUrl.toURL(), config)

            HttpRequest request = HttpRequest.GET(uri)
                    .header('Accept', MediaType.APPLICATION_JSON)

            String responseBody = client.toBlocking().retrieve(request, String)
            List<String> result = objectMapper.readValue(responseBody, List.class)

            logger.info("Fetched {} {} entries from admin-service", result?.size() ?: 0, description)
            return result ?: Collections.emptyList()

        } catch (Exception e) {
            logger.warn("Failed to fetch {} from admin-service ({}): {}", description, uri, e.getMessage())
            return null
        } finally {
            client?.close()
        }
    }
}
