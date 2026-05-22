package se.service

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.ApplicationContext
import io.micronaut.context.ApplicationContextBuilder
import io.micronaut.http.client.HttpClient
import io.micronaut.http.uri.UriBuilder
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import org.mockito.Mockito
import se.EdgeConfig
import se.service.AuthCacheService
import se.service.UtilService
import spock.lang.Shared
import spock.lang.Specification

import static org.mockito.ArgumentMatchers.any

class AuthCacheServiceSpec extends Specification {

  @Shared
  ApplicationContext context
  @Shared
  ObjectMapper objectMapper
  @Shared
  UtilService mockUtilService
  @Shared
  AuthCacheService authCacheService

  def setup() {
    ApplicationContextBuilder builder = ApplicationContext.builder()
    builder.properties([
        'se.config.handler': 'main',
        'se.config.adminServerUrl':'http://localhost:8080'
    ])
    context = builder.build()
    context.start()

    mockUtilService = Mock(UtilService)
    context.registerSingleton(UtilService, mockUtilService)

    authCacheService = context.getBean(AuthCacheService)
    objectMapper = context.getBean(ObjectMapper)
  }

  def cleanup(){
    context.close()
  }

  def "test shouldAuthenticate returns true for authorized charger"() {
    given: "a mock response from UtilService.getClientResponse"
    def responseMap = [data: [
        [chargerId: "charger1", password: "password1", authFlg: true],
        [chargerId: "charger2", password: "password2", authFlg: false]
    ]]
    String jsonResponse = objectMapper.writeValueAsString(responseMap)

    mockUtilService.getClientResponse(_ as HttpClient, _ as UriBuilder) >> jsonResponse
    String identifier = "charger1"

    when:
    boolean result = authCacheService.shouldAuthenticate(identifier)

    then:
    result
  }

  def "test shouldAuthenticate returns false for unauthorized charger"() {
    given: "a mock response from UtilService.getClientResponse"
    String identifier = "charger2"
    def responseMap = [data: [
        [chargerId: "charger1", password: "password1", authFlg: true],
        [chargerId: "charger2", password: "password2", authFlg: false]
    ]]
    String jsonResponse = objectMapper.writeValueAsString(responseMap)

    mockUtilService.getClientResponse(_ as HttpClient, _ as UriBuilder) >> jsonResponse

    when:
    boolean result = authCacheService.shouldAuthenticate(identifier)

    then:
    !result
  }

  def "fetchChargerCredentials updates cache"() {
    given: "a mock response from UtilService.getClientResponse"
    def responseMap = [data: [
        [chargerId: "charger1", password: "password1", authFlg: true],
        [chargerId: "charger2", password: "password2", authFlg: false]
    ]]
    String jsonResponse = objectMapper.writeValueAsString(responseMap)

    mockUtilService.getClientResponse(_ as HttpClient, _ as UriBuilder) >> jsonResponse

    when: "fetchChargerCredentials is called"
    def credentials = authCacheService.fetchChargerCredentials()
    authCacheService.updateChargerCredentials(credentials)

    then: "the cache is updated correctly"
    authCacheService.getChargerCredentials().get("charger1").authFlg == true
    authCacheService.getChargerCredentials().get("charger2").authFlg == false
  }

  def "test fetchChargerCredentials returns correct map"() {
    given:
    def responseMap = [data: [
        [chargerId: "EVB-1", password: "password1", authFlg: true],
        [chargerId: "EVB-2", password: "password2", authFlg: true]
    ]]
    String jsonResponse = objectMapper.writeValueAsString(responseMap)

    and: "mocking the static method UtilService.getClientResponse"
    mockUtilService.getClientResponse(_ as HttpClient, _ as UriBuilder) >> jsonResponse

    when:
    def credentials = authCacheService.fetchChargerCredentials()
    authCacheService.updateChargerCredentials(credentials)

    then:
    credentials[0].chargerId == "EVB-1"
    credentials[1].chargerId == "EVB-2"
  }
}
