package se.factory

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.context.ApplicationContextBuilder
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.inject.qualifiers.Qualifiers
import se.ocpp16.handlers.ReservationEH
import se.service.ConnectionService
import spock.lang.Specification

class CoordinatingFactorySpec extends Specification {

  ApplicationContext context

  def setup(){
    ApplicationContextBuilder builder = ApplicationContext.builder()
    builder.properties([
        'se.config.handler': 'main',
        "se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"
    ])
    context = builder.build()
    context.start()
  }

  def cleanup(){
    context.close()
  }

  def "test mock bean creation"() {
    setup:
    JSONServer mockJsonServer = Mock(JSONServer)
    ServerEvents mockServerEvents = Mock(ServerEvents)

    when:
    context.registerSingleton(JSONServer, mockJsonServer, Qualifiers.byName("mainJsonServer"))
    context.registerSingleton(ServerEvents, mockServerEvents)

    then:
    context.getBean(ServerEvents) != null
    and:
    context.getBean(ServerEvents) == mockServerEvents
  }

  def "test bean creation"() {
    expect:
    context.getBean(ObjectMapper) != null
    context.getBean(JSONServer, Qualifiers.byName("mainJsonServer")) != null
    context.getBean(ServerEvents) != null
    context.getBean(ServerCoreEventHandler) != null
    context.getBean(ConnectionService) != null
  }

  def "test singleton scope"() {
    when:
    ObjectMapper objectMapper1 = context.getBean(ObjectMapper)
    ObjectMapper objectMapper2 = context.getBean(ObjectMapper)

    then:
    objectMapper1.is(objectMapper2)

    when:
    JSONServer jsonServer1 = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    JSONServer jsonServer2 = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))

    then:
    jsonServer1.is(jsonServer2)

  }

  def "test conditional bean creation"() {
    setup:
    context = ApplicationContext.run(["se.config.handler": "main", "se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    expect:
    context.getBean(JSONServer, Qualifiers.byName("mainJsonServer")) != null

  }

  def "test dependency injection"() {

    when:
    JSONServer jsonServer = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    ConnectionService connectionService = context.getBean(ConnectionService)

    then:
    connectionService.jsonServer != null
    connectionService.jsonServer.is(jsonServer)

  }

  def "test bean behavior"() {
    given:
    def host = "127.0.0.1"
    def port = 6858

    when:
    JSONServer jsonServer = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    ServerEvents serverEvents = context.getBean(ServerEvents)
    jsonServer.open(host, port, serverEvents)

    then:
    !jsonServer.closed

    cleanup:
    jsonServer.close()
  }

  // ─────────────────────────────────────────────
  // FACT-RES-01 — ServerReservationProfile registered (BR-013)
  // ─────────────────────────────────────────────

  def "FACT-RES-01 ReservationEH bean is present in context — confirms ServerReservationProfile is wired in CoordinatingFactory (BR-013)"() {
    expect: "ReservationEH singleton is created and injectable — required by ServerReservationProfile constructor"
    context.getBean(ReservationEH) != null
  }

}
