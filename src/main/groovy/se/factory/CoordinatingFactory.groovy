package se.factory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.feature.profile.*
import eu.chargetime.ocpp.feature.profile.securityext.ServerSecurityExtProfile
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.SocketSessionEvents
import se.ocpp16.handlers.CoreProfile16EH
import se.ocpp16.handlers.FirmwareEH
import se.ocpp16.handlers.SecurityExtEH
import se.ocpp16.handlers.ReservationEH
import se.ocpp16.handlers.SessionAuthenticator
import se.service.ConnectionService
import se.service.ChargerIdentifierCacheService
import se.service.IdTagCacheService
import se.service.MtlsHandshakeListener
import se.service.UtilService

import javax.net.ssl.SSLContext
import java.util.concurrent.atomic.AtomicInteger

@Factory
//@Requires(property = "kafka.enabled", value = "true")
@Requires(property = "se.config.handler", value = "main")
class CoordinatingFactory {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatingFactory.class);

  @Value('${kafka.enabled:true}')
  boolean isKafkaEnabled

  @Inject
  ObjectMapper objectMapper

  @Inject
  OcppEventReceiver ocppIncoming

  @Inject
  MessageProducer messageProducer

  @Inject
  MeterRegistry meterRegistry

  @Inject
  FirmwareEH firmwareEventHandler

  @Inject
  SecurityExtEH securityExtEventHandler

  @Inject
  ReservationEH reservationEventHandler

  @Inject
  AtomicInteger sessionGauge

  JSONServer localServer

  @Inject
  ConnectionService connections

  @Inject
  EdgeConfig edgeConfig

  @Inject
  SSLContext sslContext

  @Inject
  SessionAuthenticator sessionAuthenticator

  @Inject
  UtilService utilService

  @Inject
  MtlsHandshakeListener mtlsHandshakeListener

  @Inject
  ChargerIdentifierCacheService chargerIdentifierCacheService

  @Inject
  IdTagCacheService idTagCacheService

  CoreProfile16EH coreProfile16EH

  @Inject
  CoordinatingFactory(ObjectMapper objectMapper,
                      OcppEventReceiver ocppIncoming,
                      MeterRegistry meterRegistry,
                      FirmwareEH firmwareEventHandler,
                      SecurityExtEH securityExtEventHandler,
                      ReservationEH reservationEventHandler,
                      AtomicInteger sessionGauge,
                      ConnectionService connections,
                      EdgeConfig edgeConfig,
                      SSLContext sslContext,
                      SessionAuthenticator sessionAuthenticator,
                      UtilService utilService) {
    this.objectMapper = objectMapper
    this.ocppIncoming = ocppIncoming
    this.meterRegistry = meterRegistry
    this.firmwareEventHandler = firmwareEventHandler
    this.securityExtEventHandler = securityExtEventHandler
    this.reservationEventHandler = reservationEventHandler
    this.sessionGauge = sessionGauge
    this.connections = connections
    this.edgeConfig = edgeConfig
    this.sslContext = sslContext
    this.sessionAuthenticator = sessionAuthenticator
    this.utilService = utilService
  }

  @Singleton
  ObjectMapper objectMapper() {
    def mapper = new ObjectMapper()
    mapper.findAndRegisterModules()
    mapper.registerModule(new JavaTimeModule())
    return mapper
  }

  @Singleton
  @Named("mainJsonServer")
  JSONServer jsonServer() {
    if (!this.localServer) {
      def bean = new JSONServer(new ServerCoreProfile(coreEventHandler()))
      bean.addFeatureProfile(new ServerRemoteTriggerProfile())
      bean.addFeatureProfile(new ServerLocalAuthListProfile())
      bean.addFeatureProfile(new ServerSmartChargingProfile())
      bean.addFeatureProfile(new ServerFirmwareManagementProfile(firmwareEventHandler))
      bean.addFeatureProfile(new ServerReservationProfile())
      bean.addFeatureProfile(new ServerSecurityExtProfile(securityExtEventHandler))
      // WS server — no enableWSS (plaintext WebSocket only)
      this.localServer = bean
    }
    this.connections.jsonServer = this.localServer

    return this.localServer
  }

  /**
   * WSS server bean — only created when WSS is enabled.
   * Uses the same OCPP feature profiles as the WS server but with TLS enabled.
   * US-CERT106-01: When mTLS enabled, uses MtlsWebSocketServerFactory for clientAuth.
   * @since US-TLS103-02, US-CERT106-01
   */
  @Singleton
  @Named("wssJsonServer")
  @Requires(property = "se.config.wss.enabled", value = "true")
  JSONServer wssJsonServer() {
    def bean = new JSONServer(new ServerCoreProfile(coreEventHandler()))
    bean.addFeatureProfile(new ServerRemoteTriggerProfile())
    bean.addFeatureProfile(new ServerLocalAuthListProfile())
    bean.addFeatureProfile(new ServerSmartChargingProfile())
    bean.addFeatureProfile(new ServerFirmwareManagementProfile(firmwareEventHandler))
    bean.addFeatureProfile(new ServerReservationProfile())
    bean.addFeatureProfile(new ServerSecurityExtProfile(securityExtEventHandler))

    if (edgeConfig.mtls.enabled) {
      bean.enableWSS(sslContext)
      logger.info("WSS JSONServer created with mTLS — clientAuth={}", edgeConfig.mtls.clientAuth)
    } else {
      bean.enableWSS(sslContext)
      logger.info("WSS JSONServer created with TLS enabled")
    }

    // Register WSS server with ConnectionService so it can route messages to WSS sessions
    this.connections.wssJsonServer = bean

    return bean
  }


  @Singleton
  ServerEvents coreEvents() {
    def bean = new SocketSessionEvents(objectMapper,
        ocppIncoming, meterRegistry,
        connections, edgeConfig,
        sessionAuthenticator, isKafkaEnabled, messageProducer,
        chargerIdentifierCacheService)
    //bean.selfcheck()
    return bean
  }

  @Singleton
  ServerCoreEventHandler coreEventHandler() {
    if (!this.coreProfile16EH) {
      def bean = new CoreProfile16EH(objectMapper: objectMapper(), ocppIncoming: ocppIncoming,
          meterRegistry: meterRegistry, connections: connections, edgeConfig: edgeConfig,
          kafkaEnabled: isKafkaEnabled, messageProducer: messageProducer,
          idTagCacheService: idTagCacheService
      )
      this.coreProfile16EH = bean
    }
    return this.coreProfile16EH
  }

}
