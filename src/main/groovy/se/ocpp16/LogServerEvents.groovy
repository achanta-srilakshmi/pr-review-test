package se.ocpp16

import eu.chargetime.ocpp.AuthenticationException
import eu.chargetime.ocpp.ServerEvents
import eu.chargetime.ocpp.model.SessionInformation
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.service.ConnectionService

class LogServerEvents implements ServerEvents {

  public static final Logger logger = LoggerFactory.getLogger(LogServerEvents.class)

  ConnectionService connections

  EdgeConfig edgeConfig

  LogServerEvents(ConnectionService connections, EdgeConfig edgeConfig) {
    this.connections = connections
    this.edgeConfig = edgeConfig
  }

  @Override
  void authenticateSession(SessionInformation information, String username, byte[] password) throws AuthenticationException {
    logger.trace("authenticateSession {}", information.properties)
    String evseIdentifier = this.deduceIdentifier(information)
    if (!evseIdentifier) {
      throw new Exception("Identifier is null")
    }
  }

  @Override
  void newSession(UUID sessionIndex, SessionInformation information) {
    String evseIdentifier = this.deduceIdentifier(information)
    logger.info("NewSession for {}", evseIdentifier)
    connections.newConnection(sessionIndex.toString(), evseIdentifier)
  }

  @Override
  void lostSession(UUID sessionIndex) {
    logger.info("{} lost a session", connections.getChargerId(sessionIndex.toString()))
    connections.lostConnection(sessionIndex.toString())
  }

  private String deduceIdentifier(SessionInformation si) {
    def parts = si.identifier?.split('/')
    return (parts.length > 0) ? parts.last() : null;
  }
}
