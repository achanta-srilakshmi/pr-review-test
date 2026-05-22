package se.service

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.uri.UriBuilder
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Singleton
class UtilService {

  private static final Logger logger = LoggerFactory.getLogger(UtilService.class)

  UtilService() {

  }

  /**
   * This method is used to extract the response from the HttpClient
   * and return it as a String
   * @param client
   * @param urlBuilder
   * @return
   */
  public String getClientResponse(HttpClient client, UriBuilder urlBuilder) {
    HttpRequest request = HttpRequest.GET(urlBuilder.build())
        .header('Accept-Version', '1.0')
        .header('Accept', MediaType.APPLICATION_JSON)
    HttpResponse<String> response = client.toBlocking().exchange(request, String)
    logger.trace("Response for GET request {} is {}", request.toString(), response.toString())
    String jsonResponse = response.body()
    return jsonResponse
  }
}
