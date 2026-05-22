package se.factory

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.inject.qualifiers.Qualifiers
import org.junit.jupiter.api.Test
import se.service.ConnectionService

import static org.junit.jupiter.api.Assertions.*

class BeanFactoryTest {

  @Test
  void testBeanCreation() {
    ApplicationContext context = ApplicationContext.run(["se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    assertNotNull(context.getBean(ObjectMapper))
    assertNotNull(context.getBean(JSONServer, Qualifiers.byName("mainJsonServer")))
    assertNotNull(context.getBean(ServerEvents))
    assertNotNull(context.getBean(ServerCoreEventHandler))
    assertNotNull(context.getBean(ConnectionService))

    context.close()
  }

  @Test
  void testSingletonScope() {
    ApplicationContext context = ApplicationContext.run(["se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    ObjectMapper objectMapper1 = context.getBean(ObjectMapper)
    ObjectMapper objectMapper2 = context.getBean(ObjectMapper)
    assertSame(objectMapper1, objectMapper2)

    JSONServer jsonServer1 = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    JSONServer jsonServer2 = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    assertSame(jsonServer1, jsonServer2)

    context.close()
  }

  @Test
  void testConditionalBeanCreation() {
    ApplicationContext context = ApplicationContext.run(["se.config.handler": "main", "se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    assertNotNull(context.getBean(JSONServer, Qualifiers.byName("mainJsonServer")))

    context.close()
  }

  //@Test: TODO: Need to figure out how to fix this
  void testConditionalBeanNotCreated() {
    ApplicationContext context = ApplicationContext.run(["se.config.handler": "other"])

    assertThrows(NoSuchBeanException, { context.getBean(JSONServer, Qualifiers.byName("mainJsonServer")) })

    context.close()
  }

  @Test
  void testDependencyInjection() {
    ApplicationContext context = ApplicationContext.run(["se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    JSONServer jsonServer = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    ConnectionService connectionService = context.getBean(ConnectionService)

    assertNotNull(connectionService.jsonServer)
    assertSame(jsonServer, connectionService.jsonServer)

    context.close()
  }

  @Test
  void testBeanBehavior() {
    ApplicationContext context = ApplicationContext.run(["se.config.wss.keyStore": "src/test/resources/evoke-keystore.p12"])

    JSONServer jsonServer = context.getBean(JSONServer, Qualifiers.byName("mainJsonServer"))
    ServerEvents serverEvents = context.getBean(ServerEvents)
    jsonServer.open("127.0.0.1", 8989, serverEvents)

    assertFalse(jsonServer.closed)
    //assertTrue(serverEvents.isRunning())

    jsonServer.close()
    context.close()
  }




}
