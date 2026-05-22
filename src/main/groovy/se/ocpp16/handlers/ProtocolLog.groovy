package se.ocpp16.handlers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.slf4j.MarkerFactory

class ProtocolLog {
  private static final Logger logger = LoggerFactory.getLogger(ProtocolLog.class)

  public static void debug(boolean incoming, String sessionId, String msg){
    MDC.put("sessionId", "[${incoming?"Received":"Sent"}]:[${sessionId}]")
    logger.debug(MarkerFactory.getMarker("sessionId"), msg?:"EMPTY")
    MDC.clear()
  }

  public static void info(boolean incoming, String chargerId, Integer messageTypeId, String sessionId, String action, String payloadStr) {
    String logMsg = "[${messageTypeId}, \"${sessionId}\", \"${action}\", ${payloadStr} ]"
    MDC.put("sessionId", "[${incoming?"Received":"Sent"}]:[chargerId=${chargerId}]")
    logger.info(MarkerFactory.getMarker("sessionId"), logMsg?:"EMPTY")
    MDC.clear()
  }
}
