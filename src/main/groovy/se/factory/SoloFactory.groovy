package se.factory

import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.feature.profile.*
import eu.chargetime.ocpp.feature.profile.securityext.ServerSecurityExtProfile
import se.ocpp16.handlers.ReservationEH
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.ocpp16.LogServerEvents
import se.ocpp16.handlers.SecurityExtEH
import se.ocpp16.handlers.SoloEventHandler
import se.service.ConnectionService
import se.service.MtlsHandshakeListener

import javax.net.ssl.SSLContext

@Factory
@Requires(property = "se.config.handler", value = "solo")
class SoloFactory {

  private static final Logger logger = LoggerFactory.getLogger(SoloFactory.class)

  JSONServer localJsonServer

  @Inject
  ConnectionService connections

  @Inject
  SoloEventHandler soloEventHandler

  @Inject
  SecurityExtEH securityExtEventHandler

  @Inject
  ReservationEH reservationEventHandler

  @Inject
  EdgeConfig edgeConfig

  @Inject
  SSLContext sslContext

  @Inject
  MtlsHandshakeListener mtlsHandshakeListener

  @Singleton
  ServerEvents coreEvents() {
    return new LogServerEvents(connections, edgeConfig)
  }

  @Singleton
  @Named("mainJsonServer")
  JSONServer jsonServer() {
    if (!this.localJsonServer) {
      def bean = new JSONServer(new ServerCoreProfile(soloEventHandler))
      bean.addFeatureProfile(new ServerRemoteTriggerProfile())
      bean.addFeatureProfile(new ServerLocalAuthListProfile())
      bean.addFeatureProfile(new ServerSmartChargingProfile())
      //bean.addFeatureProfile(new ServerFirmwareManagementProfile(firmwareEventHandler))
      bean.addFeatureProfile(new ServerReservationProfile())
      bean.addFeatureProfile(new ServerSecurityExtProfile(securityExtEventHandler))
      // WS server — no enableWSS (plaintext WebSocket only)
      this.localJsonServer = bean
    }
    this.connections.jsonServer = this.localJsonServer
    return this.localJsonServer
  }

  /**
   * WSS server bean — only created when WSS is enabled.
   * US-CERT106-01: When mTLS enabled, uses MtlsWebSocketServerFactory for clientAuth.
   * @since US-TLS103-02, US-CERT106-01
   */
  @Singleton
  @Named("wssJsonServer")
  @Requires(property = "se.config.wss.enabled", value = "true")
  JSONServer wssJsonServer() {
    def bean = new JSONServer(new ServerCoreProfile(soloEventHandler))
    bean.addFeatureProfile(new ServerRemoteTriggerProfile())
    bean.addFeatureProfile(new ServerLocalAuthListProfile())
    bean.addFeatureProfile(new ServerSmartChargingProfile())
    bean.addFeatureProfile(new ServerReservationProfile())
    bean.addFeatureProfile(new ServerSecurityExtProfile(securityExtEventHandler))
    bean.enableWSS(sslContext)

    if (edgeConfig.mtls.enabled) {
      configureMtlsFactory(bean)
      logger.info("WSS JSONServer (solo) created with mTLS — clientAuth={}", edgeConfig.mtls.clientAuth)
    } else {
      configureOcppSslFactory(bean)
      logger.info("WSS JSONServer (solo) created with TLS enabled")
    }
    return bean
  }

  /**
   * Configures OCPP-compliant SSL factory for TLS 1.2+ and approved cipher suites.
   * @since US-TLS103-02 (TC-66)
   */
  private void configureOcppSslFactory(JSONServer server) {
    try {
      def listenerField = server.getClass().getDeclaredField('listener')
      listenerField.setAccessible(true)
      def listener = listenerField.get(server)
      if (listener != null) {
        def wsServer = findWebSocketServer(listener)
        if (wsServer != null) {
          def ocppFactory = new OcppSslWebSocketServerFactory(sslContext)
          def wsfField = org.java_websocket.server.WebSocketServer.getDeclaredField('wsfactory')
          wsfField.setAccessible(true)
          wsfField.set(wsServer, ocppFactory)
          logger.info("OCPP SSL WebSocketServerFactory installed (solo)")
        }
      }
    } catch (Exception e) {
      logger.error("Failed to configure OCPP SSL factory (solo): {}", e.message, e)
    }
  }

  private void configureMtlsFactory(JSONServer server) {
    try {
      def listenerField = server.getClass().getDeclaredField('listener')
      listenerField.setAccessible(true)
      def listener = listenerField.get(server)
      if (listener != null) {
        def wsServer = findWebSocketServer(listener)
        if (wsServer != null) {
          def mtlsFactory = new MtlsWebSocketServerFactory(sslContext, edgeConfig.mtls.clientAuth, mtlsHandshakeListener)
          def wsfField = org.java_websocket.server.WebSocketServer.getDeclaredField('wsfactory')
          wsfField.setAccessible(true)
          wsfField.set(wsServer, mtlsFactory)
          logger.info("mTLS WebSocketServerFactory installed (solo) — clientAuth={}", edgeConfig.mtls.clientAuth)
        }
      }
    } catch (Exception e) {
      logger.error("Failed to configure mTLS factory (solo): {}", e.message, e)
    }
  }

  private static org.java_websocket.server.WebSocketServer findWebSocketServer(Object obj) {
    if (obj instanceof org.java_websocket.server.WebSocketServer) {
      return (org.java_websocket.server.WebSocketServer) obj
    }
    for (def field : obj.getClass().getDeclaredFields()) {
      field.setAccessible(true)
      try {
        def value = field.get(obj)
        if (value instanceof org.java_websocket.server.WebSocketServer) {
          return (org.java_websocket.server.WebSocketServer) value
        }
      } catch (Exception ignored) {}
    }
    def superClass = obj.getClass().getSuperclass()
    while (superClass != null && superClass != Object) {
      for (def field : superClass.getDeclaredFields()) {
        field.setAccessible(true)
        try {
          def value = field.get(obj)
          if (value instanceof org.java_websocket.server.WebSocketServer) {
            return (org.java_websocket.server.WebSocketServer) value
          }
        } catch (Exception ignored) {}
      }
      superClass = superClass.getSuperclass()
    }
    return null
  }

}
