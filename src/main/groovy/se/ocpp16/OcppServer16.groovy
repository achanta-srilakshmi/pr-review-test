package se.ocpp16

import eu.chargetime.ocpp.JSONServer
import eu.chargetime.ocpp.ServerEvents
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.annotation.PostConstruct
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.java_websocket.drafts.Draft
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.factory.BeanFactory
import se.factory.MtlsWebSocketServerFactory
import se.factory.OcppSslWebSocketServerFactory
import se.service.MtlsHandshakeListener

import javax.annotation.Nullable
import javax.net.ssl.SSLContext

/**
 * The primary class responsible for opening WebSocket servers (WS + WSS).
 *
 * US-TLS103-02: Opens both a plaintext WS port and a TLS-secured WSS port
 * when WSS is enabled. The WS port is always opened for backward compatibility.
 *
 * US-SEC104-01: Configures OCPP 1.6 subprotocol negotiation on both WS and WSS
 * servers after open(). Supports strict and permissive modes.
 */
@Singleton
class OcppServer16 {

  public static final Logger logger = LoggerFactory.getLogger(OcppServer16.class)

  @Inject
  ServerEvents coreEvents

  @Inject
  @Named("mainJsonServer")
  JSONServer jsonServer

  @Inject
  @Nullable
  @Named("wssJsonServer")
  JSONServer wssJsonServer

  @Inject
  EdgeConfig edgeConfig

  @Inject
  SSLContext sslContext

  @Inject
  MtlsHandshakeListener mtlsHandshakeListener

  @PostConstruct
  void started() throws Exception {
    logger.info("SSL config {}", edgeConfig.wss.properties)

    // Always open the WS (plaintext) port
    jsonServer.open("0.0.0.0", edgeConfig.webSocketPort, this.coreEvents)
    configureSubprotocol(jsonServer, "WS")
    logger.info("WS binding on port {}", edgeConfig.webSocketPort)

    // Open the WSS (TLS) port if enabled and the wssJsonServer bean exists
    if (edgeConfig.wss.enabled && wssJsonServer != null) {
      wssJsonServer.open("0.0.0.0", edgeConfig.wss.wssPort, this.coreEvents)
      configureSubprotocol(wssJsonServer, "WSS")
      configureSslFactory(wssJsonServer)
      logger.info("WSS binding on port {} with TLS 1.2+", edgeConfig.wss.wssPort)
    }

    logger.info("Subprotocol negotiation: strict={}, supported='{}'",
        edgeConfig.subprotocol.strict, BeanFactory.OCPP16_SUBPROTOCOL)

    logger.info("Websocket binding complete — WS:{} WSS:{} ssl:{}",
        edgeConfig.webSocketPort,
        edgeConfig.wss.enabled ? edgeConfig.wss.wssPort : "disabled",
        edgeConfig.wss.enabled)
  }

  /**
   * Configures OCPP-compliant SSL factory on the WSS server after open().
   * Replaces the default SSLWebSocketServerFactory with either OcppSslWebSocketServerFactory
   * (TLS mode) or MtlsWebSocketServerFactory (mTLS mode) to enforce cipher suites and protocols.
   *
   * Must be called AFTER open() because the listener is only created during open().
   *
   * @since US-TLS103-02 (TC-66)
   */
  private void configureSslFactory(JSONServer server) {
    try {
      def listenerField = server.getClass().getDeclaredField('listener')
      listenerField.setAccessible(true)
      def listener = listenerField.get(server)

      if (listener == null) {
        logger.warn("Could not configure SSL factory: listener is null")
        return
      }

      def wsServer = findWebSocketServer(listener)
      if (wsServer == null) {
        logger.warn("Could not find WebSocketServer to install SSL factory")
        return
      }

      // Find the WebSocketServerFactory field by type (field name varies by library version)
      def wsfField = null
      def searchClass = wsServer.getClass()
      while (searchClass != null && searchClass != Object) {
        for (def f : searchClass.getDeclaredFields()) {
          f.setAccessible(true)
          try {
            def val = f.get(wsServer)
            if (val != null && val.getClass().name.contains('SSLWebSocketServerFactory')) {
              wsfField = f
              break
            }
          } catch (Exception ignored) {}
        }
        if (wsfField != null) break
        searchClass = searchClass.getSuperclass()
      }

      if (wsfField == null) {
        // Fallback: search for any field of type WebSocketServer.WebSocketServerFactory or similar
        logger.warn("Could not find SSL factory field by type. Listing all fields for debug:")
        def sc = wsServer.getClass()
        while (sc != null && sc != Object) {
          for (def f : sc.getDeclaredFields()) {
            logger.debug("  Field: {} type: {}", f.name, f.type.name)
          }
          sc = sc.getSuperclass()
        }
        return
      }

      if (edgeConfig.mtls.enabled) {
        def mtlsFactory = new MtlsWebSocketServerFactory(sslContext, edgeConfig.mtls.clientAuth, mtlsHandshakeListener)
        wsfField.set(wsServer, mtlsFactory)
        logger.info("mTLS WebSocketServerFactory installed — clientAuth={}", edgeConfig.mtls.clientAuth)
      } else {
        def ocppFactory = new OcppSslWebSocketServerFactory(sslContext)
        wsfField.set(wsServer, ocppFactory)
        logger.info("OCPP SSL WebSocketServerFactory installed — TLS 1.2+, approved ciphers only")
      }
    } catch (Exception e) {
      logger.error("Failed to configure SSL factory: {}", e.message, e)
    }
  }

  /**
   * Configures OCPP 1.6 subprotocol negotiation on the internal WebSocket server.
   * Replaces the default Draft_6455 (no subprotocol) with OcppSubprotocolDraft
   * which validates Sec-WebSocket-Protocol header per OCPP 1.6 Part 2, Section 3.
   *
   * Uses reflection to access the internal Java-WebSocket server's drafts list
   * because the eu.chargetime.ocpp library does not expose draft configuration.
   *
   * @param server the JSONServer whose internal WebSocket server to configure
   * @param label "WS" or "WSS" for logging
   * @since US-SEC104-01 (TC-67)
   */
  private void configureSubprotocol(JSONServer server, String label) {
    try {
      // Access JSONServer.listener (WebSocketListener)
      def listenerField = server.getClass().getDeclaredField('listener')
      listenerField.setAccessible(true)
      def listener = listenerField.get(server)

      if (listener == null) {
        logger.warn("Could not configure {} subprotocol: listener is null", label)
        return
      }

      // The listener may or may not extend WebSocketServer directly.
      // Find the actual WebSocketServer instance to set drafts on.
      def wsServer = findWebSocketServer(listener)
      if (wsServer == null) {
        logger.warn("Could not find WebSocketServer in {} listener of type {}", label, listener.getClass().name)
        return
      }

      // Access WebSocketServer.drafts (private List<Draft>)
      def draftsField = org.java_websocket.server.WebSocketServer.getDeclaredField('drafts')
      draftsField.setAccessible(true)

      def customDraft = new OcppSubprotocolDraft(edgeConfig.subprotocol.strict)
      draftsField.set(wsServer, [customDraft] as List<Draft>)

      logger.info("{} subprotocol configured: supported='{}', strict={}",
          label, BeanFactory.OCPP16_SUBPROTOCOL, edgeConfig.subprotocol.strict)
    } catch (Exception e) {
      logger.error("Failed to configure {} subprotocol negotiation: {}",
          label, e.message, e)
    }
  }

  /**
   * Finds the WebSocketServer instance, either the object itself or nested inside it.
   * After enableWSS(), the listener wraps a WebSocketServer rather than extending it.
   */
  private static org.java_websocket.server.WebSocketServer findWebSocketServer(Object obj) {
    // If it IS a WebSocketServer, return directly
    if (obj instanceof org.java_websocket.server.WebSocketServer) {
      return (org.java_websocket.server.WebSocketServer) obj
    }
    // Otherwise, search declared fields for a WebSocketServer instance
    for (def field : obj.getClass().getDeclaredFields()) {
      field.setAccessible(true)
      try {
        def value = field.get(obj)
        if (value instanceof org.java_websocket.server.WebSocketServer) {
          return (org.java_websocket.server.WebSocketServer) value
        }
      } catch (Exception ignored) {}
    }
    // Try superclass fields as well
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

  @EventListener
  public void onShutdownEvent(ShutdownEvent event) {
    logger.warn("Shutting down the socket edge component")
    jsonServer.close()
    if (wssJsonServer != null) {
      wssJsonServer.close()
      logger.info("WSS server closed")
    }
  }
}
