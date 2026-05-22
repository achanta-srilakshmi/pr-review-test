package se.controller

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.core.*
import eu.chargetime.ocpp.model.firmware.GetDiagnosticsConfirmation
import eu.chargetime.ocpp.model.firmware.GetDiagnosticsRequest
import eu.chargetime.ocpp.model.localauthlist.GetLocalListVersionConfirmation
import eu.chargetime.ocpp.model.localauthlist.GetLocalListVersionRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageConfirmation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequestType
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.context.annotation.Property
import io.micronaut.context.env.Environment
import javax.annotation.Nullable
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.*
import jakarta.inject.Inject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.service.ConnectionService

@Controller("/ocpp16/v1")
public class ChargerController {

  @Inject
  MeterRegistry meterRegistry

  @Inject
  ObjectMapper objectMapper

  @Property(name = "se.diagnostics.location", defaultValue = "")
  String diagnosticLogLoc

  @Inject
  private Environment environment

  @Inject
  ConnectionService connections

  public static final Logger logger = LoggerFactory.getLogger(ChargerController.class)

  @Get("/")
  @Produces(MediaType.APPLICATION_JSON)
  def ping() {
    def version = se.Version?.getVersion()
    return [version: version]
  }

  /**
   * The curl command to get the sessionId for a charger is:
   * curl -X GET "http://localhost:9465/ocpp16/v1/sessionId?identifier=1" -H "accept: application/json"
   * @param identifier
   * @return
   */
  @Get("/sessionId")
  @Produces(MediaType.APPLICATION_JSON)
  def getSessionId(@QueryValue String identifier){
    return [sessionId: connections.getLatestSessionId(identifier)?:"NO_SESSION"]
  }

  /**
   *
   * The curl command to get the chargerId for a session is:
   * curl -X GET "http://localhost:9465/ocpp16/v1/chargerId?sessionId=1b4e5e52-3b6d-4e1f-9b2b-3f6e0b1b1b1b" -H "accept: application/json"
   *
   * @param sessionId
   * @return
   */
  @Get("/chargerId")
  @Produces(MediaType.APPLICATION_JSON)
  def getChargerIdForSession(@QueryValue String sessionId){
    return [chargerId: connections.getChargerId(sessionId)?:"NO_CHARGER"]
  }

  @Get("/getChargerConfiguration")
  @Produces(MediaType.APPLICATION_JSON)
  def getConfiguration(@QueryValue String identifier, @Nullable @QueryValue String[] keys) {
    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString){
      meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
      return [status: 'FAIL', statusText: "No session found"]
    }
    def sessionId = UUID.fromString(sessionString)
    logger.trace("Attempting to fetch configuration for charger with identifier ${identifier}")
    try {
      GetConfigurationRequest request = new GetConfigurationRequest()
      if(keys){
        logger.info("Keys length $keys.length, and keys are $keys")
        request.setKey(keys)
      }
      List configuration = ((GetConfigurationConfirmation) connections.callInline(sessionId, request))?.configurationKey?.collect({
        [key: it.key, value: it.value, readonly: it.readonly]
      })
      logger.trace("Fetched configuration " + configuration)
      return [configurationKey: configuration]
    } catch (NotConnectedException e) {
      logger.error("Configuration cannot be retrieved for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while fetching configuration ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/getLocalListVersion")
  @Produces(MediaType.APPLICATION_JSON)
  def getLocalListVersion(@QueryValue String identifier) {
    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString){
      meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
      return [status: 'FAIL', statusText: "No session found"]
    }
    def sessionId = UUID.fromString(sessionString)
    logger.debug("Attempting to fetch local list version for charger with identifier ${identifier}")
    try {
      GetLocalListVersionRequest request = new GetLocalListVersionRequest()

      GetLocalListVersionConfirmation confirmation = connections.callInline(sessionId, request)
      def localListVersion = confirmation?.getListVersion()
      logger.trace("Fetched local list version: " + localListVersion)
      return [localListVersion: localListVersion]
    } catch (NotConnectedException e) {
      logger.error("Local list version cannot be retrieved for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while fetching local list version ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Put("/updateConfiguration")
  @Produces(MediaType.APPLICATION_JSON)
  def updateConfiguration(Map params) {
    logger.trace("Request to update configuration received ${params}")

    if (!params.identifier) {
      return [status: 'FAIL', statusText: 'Missing identifier']
    }
    if (!params.key) {
      return [status: 'FAIL', statusText: 'Missing key']
    }

    def sessionString = connections.getLatestSessionId(params.identifier)

    if (!sessionString){
      meterRegistry.counter("se.sessionmap.nomatch", "identifier", params.identifier).increment()
      return [status: 'FAIL', statusText: "No session found"]
    }

    def sessionId = UUID.fromString(sessionString)

    try {
      ChangeConfigurationRequest request = new ChangeConfigurationRequest(key: params.key, value: params.value ?: "")
      ChangeConfigurationConfirmation configurationConfirmation = (ChangeConfigurationConfirmation) connections.callInline(sessionId, request)

      return [status: 'SUCCESS', data: configurationConfirmation]
    } catch (NotConnectedException e) {
      logger.error("Configuration cannot be updated for an offline charger with identifier ${params.identifier}", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while updating configuration for identifier ${params.identifier}", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }


  @Get("/socketReconnect")
  @Produces(MediaType.APPLICATION_JSON)
  def socketReconnect(@QueryValue String identifier) {
    def sessionId = connections.getLatestSessionId(identifier)
    logger.trace("Request to reset socket connection received for ${sessionId}")

    try {
      connections.closeSession(UUID.fromString(sessionId))
      def reason = ["message": "SE closing socket on a Client's call"]

      return [status: 'SUCCESS']
    } catch (NotConnectedException e) {
      logger.error("The socket cannot be reset for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while resetting socket ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * Reconnects the socket with the given session id
   *
   * The curl command to test this endpoint is:
   * curl -X GET "http://localhost:9465/reconnectSocketWithSessionId?sessionId=7fcfb55b-04e9-476b-877e-5256869c4ff8"
   *
   * @param sessionId
   * @return
   */
  @Get("/reconnectSocketWithSessionId")
  @Produces(MediaType.APPLICATION_JSON)
  def reconnectSocketWithSessionId(@QueryValue UUID sessionId) {
    logger.trace("Request to reset socket connection received for ${sessionId}")

    try {
      connections.closeSession(sessionId)
      def reason = ["message": "SE closing socket on a Client's call"]

      return [status: 'SUCCESS']
    } catch (NotConnectedException e) {
      logger.error("The socket cannot be reset for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while resetting socket ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/refreshChargerMap")
  @Produces(MediaType.APPLICATION_JSON)
  def refreshChargerMap() {
    logger.trace("Request to refresh charger map received")
    def count = connections.refreshChargerMap()
    return [status: 'SUCCESS', count: count]
  }

  @Get("/requestUpdatedStatus")
  @Produces(MediaType.APPLICATION_JSON)
  def requestStatusUpdate(@QueryValue String identifier) {
    logger.trace("Request to update charger status received for ${identifier}")
    def sessionId = connections.getLatestSessionId(identifier)
    try {
      TriggerMessageRequest request = new TriggerMessageRequest()

      request.setRequestedMessage(TriggerMessageRequestType.StatusNotification)

      def confirmation = (TriggerMessageConfirmation) connections.callInline(UUID.fromString(sessionId), request)

      return confirmation
    } catch (NotConnectedException e) {
      logger.error("Status cannot be requested for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while requesting status ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/requestMeterValues")
  @Produces(MediaType.APPLICATION_JSON)
  def requestMeterValues(@QueryValue String identifier, int connectorId) {
    def sessionString = connections.getLatestSessionId(identifier)
    logger.trace("MeterValue request received for charger with identifier ${identifier}")

    try {
      TriggerMessageRequest request = new TriggerMessageRequest()
      request.connectorId = connectorId
      request.setRequestedMessage(TriggerMessageRequestType.MeterValues)
      def confirmation = (TriggerMessageConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      return confirmation
    } catch (NotConnectedException e) {
      logger.error("MeterValues cannot be requested for an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while requesting MeterValues ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/requestBoot")
  @Produces(MediaType.APPLICATION_JSON)
  def requestBoot(@QueryValue String identifier) {
    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString){
      meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
      return [status: 'FAIL', statusText: "No session found"]
    }

    def sessionId = UUID.fromString(sessionString)
    logger.trace("Request to reboot charger received for ${sessionId}")
    try {
      TriggerMessageRequest request = new TriggerMessageRequest()

      request.setRequestedMessage(TriggerMessageRequestType.BootNotification)

      def confirmation = (TriggerMessageConfirmation) connections.callInline(sessionId, request)

      return confirmation
    } catch (NotConnectedException e) {
      logger.error("Cannot communicate with an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while requesting for boot notification ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/reset")
  @Produces(MediaType.APPLICATION_JSON)
  def resetCharger(@QueryValue String identifier, @QueryValue boolean hard) {
    logger.trace("Request to ${hard ? "hard" : "soft"} reset charger received for ${identifier}")

    def sessionString = connections.getLatestSessionId(identifier)
    try {
      ResetRequest request = new ResetRequest();
      request.setType(hard ? ResetType.Hard : ResetType.Soft);

      def confirmation = (ResetConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      return confirmation
    } catch (NotConnectedException e) {
      logger.error("Cannot communicate with an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while resetting charger ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  @Get("/requestDiagnostics")
  @Produces(MediaType.APPLICATION_JSON)
  def requestDiagnostics(@QueryValue String identifier) {
    logger.trace("Request to fetch diagnostic logs received for ${identifier}")

    def sessionString = connections.getLatestSessionId(identifier)
    try {
      GetDiagnosticsRequest request = new GetDiagnosticsRequest()
      request.setLocation(diagnosticLogLoc)

      GetDiagnosticsConfirmation confirmation = connections.callInline(UUID.fromString(sessionString), request)

      return confirmation
    } catch (NotConnectedException e) {
      logger.error("Cannot communicate with an offline charger ", e)
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("Unknown error while requesting diganostic logs ", e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  //This is not to be exposed as API and is meant only for internal Use
  /**
   *
   * @param sessionId
   * @param connectorIndex
   * @param idTag : RFID used to Start
   * @return
   */
  @Post("/chargers/{sessionId}/connectors/{connectorIndex}")
  @Produces(MediaType.APPLICATION_JSON)
  def triggerRemoteStart(@PathVariable(name = 'sessionId') UUID sessionId, Integer connectorIndex) {
    logger.debug("Triggering remote start on connector ${connectorIndex} of session ${sessionId}")

    RemoteStartTransactionRequest request = new RemoteStartTransactionRequest(connectorId: connectorIndex, idTag: "abc")
    RemoteStartTransactionConfirmation confirmation = connections.callInline(sessionId, request)

    return confirmation
  }

  /**
   *
   * @param sessionId
   * @param connectorIndex
   * @param seTransactionId : TransactionId to Stop
   * @return
   */
  @Put("/chargers/{sessionId}/transactions/{seTransactionId}")
  @Produces(MediaType.APPLICATION_JSON)
  def triggerRemoteStop(@PathVariable(name = 'sessionId') UUID sessionId, Integer seTransactionId) {
    RemoteStopTransactionRequest request = new RemoteStopTransactionRequest(transactionId: seTransactionId)

    RemoteStopTransactionConfirmation confirmation = connections.callInline(sessionId, request)

    return confirmation
  }

  @Post("/chargers/lock")
  @Produces(MediaType.APPLICATION_JSON)
  @Deprecated // Use POST /chargers/unlock instead — path was semantically incorrect
  def unlockConnector(@Body Map body) {
    logger.debug("Unlocking connector ${body.connectorIndex} on ${body.sessionId}")
    UnlockConnectorRequest request = new UnlockConnectorRequest(connectorId: body['connectorIndex'])
    UnlockConnectorConfirmation unlockConfirmation = connections.callInline(UUID.fromString(body['sessionId']), request)

    return unlockConfirmation
  }

  /**
   * Sends an UnlockConnector request to the specified charge point.
   * [OCPP-COMPLIANCE] OCPP 1.6 §6.21
   *
   * connectorId MUST be > 0. ConnectorId 0 refers to the whole charge point and is
   * not permitted for UnlockConnector per the OCPP 1.6 specification.
   *
   * @param body JSON: { "sessionId": "<UUID>", "connectorId": <integer > 0> }
   */
  @Post("/chargers/unlock")
  @Produces(MediaType.APPLICATION_JSON)
  def unlockConnectorV2(@Body Map body) {
    Integer connectorId = body['connectorId'] as Integer
    String sessionIdStr = body['sessionId']

    if (!sessionIdStr) {
      logger.warn("UnlockConnector request rejected — missing sessionId")
      return [status: 'FAIL', statusText: 'Missing sessionId']
    }

    // OCPP 1.6 §6.21: connectorId MUST be > 0
    if (connectorId == null || connectorId <= 0) {
      logger.warn("UnlockConnector request rejected for session {} — invalid connectorId: {}. " +
              "OCPP 1.6 §6.21 requires connectorId > 0.", sessionIdStr, connectorId)
      return [status: 'FAIL', statusText: 'connectorId must be an integer greater than 0']
    }

    logger.debug("Unlocking connector {} on session {}", connectorId, sessionIdStr)
    UnlockConnectorRequest request = new UnlockConnectorRequest(connectorId: connectorId)
    UnlockConnectorConfirmation unlockConfirmation = connections.callInline(UUID.fromString(sessionIdStr), request)

    return unlockConfirmation
  }

  @Get("/all/updatestatus")
  @Produces(MediaType.APPLICATION_JSON)
  def updateAllStatues() {
    connections.refreshStatuses()
    return [message: "UPDATING STATUS OF ALL CHARGERS"]
  }

  // =====================================================================
  // Certificate Management Test APIs (TC-57, TC-58, TC-59, TC-68)
  // For manual testing without Kafka. NOT for production use.
  // Direct OCPP calls via ConnectionService — no Kafka dependency.
  // =====================================================================

  /**
   * TC-57: Install ManufacturerRootCertificate on a charge point.
   * TC-58: Install CentralSystemRootCertificate on a charge point.
   *
   * curl -X POST "http://localhost:9465/ocpp16/v1/installCertificate" \
   *   -H "Content-Type: application/json" \
   *   -d '{"identifier":"EVB-LOCAL-1","certificateType":"ManufacturerRootCertificate","certificate":"-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----"}'
   */
  @Post("/installCertificate")
  @Produces(MediaType.APPLICATION_JSON)
  def installCertificate(@Body Map body) {
    String identifier = body.identifier
    if (!identifier) return [status: 'FAIL', statusText: 'Missing identifier']
    if (!body.certificateType) return [status: 'FAIL', statusText: 'Missing certificateType']
    if (!body.certificate) return [status: 'FAIL', statusText: 'Missing certificate']

    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def certType = eu.chargetime.ocpp.model.securityext.types.CertificateUseEnumType.valueOf(body.certificateType as String)
      def request = new eu.chargetime.ocpp.model.securityext.InstallCertificateRequest(certType, body.certificate as String)
      def confirmation = (eu.chargetime.ocpp.model.securityext.InstallCertificateConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      logger.info("InstallCertificate for {} — type: {}, status: {}", identifier, body.certificateType, confirmation?.status)
      return [chargePointId: identifier, certificateType: body.certificateType, status: confirmation?.status?.toString() ?: "Error"]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("InstallCertificate error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * TC-59: Delete a specific certificate from the charge point.
   *
   * curl -X POST "http://localhost:9465/ocpp16/v1/deleteCertificate" \
   *   -H "Content-Type: application/json" \
   *   -d '{"identifier":"EVB-LOCAL-1","hashAlgorithm":"SHA256","issuerNameHash":"abc123","issuerKeyHash":"def456","serialNumber":"001"}'
   */
  @Post("/deleteCertificate")
  @Produces(MediaType.APPLICATION_JSON)
  def deleteCertificate(@Body Map body) {
    String identifier = body.identifier
    if (!identifier) return [status: 'FAIL', statusText: 'Missing identifier']

    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def hashData = new eu.chargetime.ocpp.model.securityext.types.CertificateHashDataType()
      hashData.hashAlgorithm = eu.chargetime.ocpp.model.securityext.types.HashAlgorithmEnumType.valueOf(body.hashAlgorithm as String)
      hashData.issuerNameHash = body.issuerNameHash as String
      hashData.issuerKeyHash = body.issuerKeyHash as String
      hashData.serialNumber = body.serialNumber as String
      def request = new eu.chargetime.ocpp.model.securityext.DeleteCertificateRequest(hashData)
      def confirmation = (eu.chargetime.ocpp.model.securityext.DeleteCertificateConfirmation) cnonnections.callInline(UUID.fromString(sessionString), request)

      logger.info("DeleteCertificate for {} — status: {}", identifier, confirmation?.status)
      return [chargePointId: identifier, status: confirmation?.status?.toString() ?: "Error"]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("DeleteCertificate error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * Get installed certificate IDs from a charge point.
   *
   * curl -X GET "http://localhost:9465/ocpp16/v1/getInstalledCertificateIds?identifier=EVB-LOCAL-1&certificateType=CentralSystemRootCertificate"
   */
  @Get("/getInstalledCertificateIds")
  @Produces(MediaType.APPLICATION_JSON)
  def getInstalledCertificateIds(@QueryValue String identifier, @QueryValue String certificateType) {
    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def certType = eu.chargetime.ocpp.model.securityext.types.CertificateUseEnumType.valueOf(certificateType)
      def request = new eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsRequest(certType)
      def confirmation = (eu.chargetime.ocpp.model.securityext.GetInstalledCertificateIdsConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      def certHashList = confirmation?.certificateHashData?.collect { hashData ->
        [hashAlgorithm: hashData.hashAlgorithm?.toString(), issuerNameHash: hashData.issuerNameHash,
         issuerKeyHash: hashData.issuerKeyHash, serialNumber: hashData.serialNumber]
      } ?: []

      logger.info("GetInstalledCertificateIds for {} — status: {}, count: {}", identifier, confirmation?.status, certHashList.size())
      return [chargePointId: identifier, status: confirmation?.status?.toString() ?: "Error", certificateHashData: certHashList]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("GetInstalledCertificateIds error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * TC-68: Trigger certificate renewal — sends ExtendedTriggerMessage(SignCertificate).
   *
   * curl -X GET "http://localhost:9465/ocpp16/v1/triggerCertificateRenewal?identifier=EVB-LOCAL-1"
   */
  @Get("/triggerCertificateRenewal")
  @Produces(MediaType.APPLICATION_JSON)
  def triggerCertificateRenewal(@QueryValue String identifier) {
    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def request = new eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageRequest(
          eu.chargetime.ocpp.model.securityext.types.MessageTriggerEnumType.SignChargePointCertificate)
      def confirmation = (eu.chargetime.ocpp.model.securityext.ExtendedTriggerMessageConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      logger.info("ExtendedTriggerMessage(SignCertificate) for {} — status: {}", identifier, confirmation?.status)
      return [chargePointId: identifier, status: confirmation?.status?.toString() ?: "Error"]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("TriggerCertificateRenewal error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * TC-61: Get Security Log from a charge point.
   *
   * curl -X POST "http://localhost:9465/ocpp16/v1/getLog" \
   *   -H "Content-Type: application/json" \
   *   -d '{"identifier":"EVB-LOCAL-1","logType":"SecurityLog","requestId":1,"remoteLocation":"https://example.com/upload"}'
   *
   * Optional fields: oldestTimestamp, latestTimestamp (ISO-8601), retries, retryInterval
   */
  @Post("/getLog")
  @Produces(MediaType.APPLICATION_JSON)
  def getLog(@Body Map body) {
    String identifier = body.identifier
    if (!identifier) return [status: 'FAIL', statusText: 'Missing identifier']
    if (!body.logType) return [status: 'FAIL', statusText: 'Missing logType']
    if (!body.requestId) return [status: 'FAIL', statusText: 'Missing requestId']
    if (!body.remoteLocation) return [status: 'FAIL', statusText: 'Missing remoteLocation']

    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def logParams = new eu.chargetime.ocpp.model.securityext.types.LogParametersType()
      logParams.remoteLocation = body.remoteLocation as String
      if (body.oldestTimestamp) {
        logParams.oldestTimestamp = java.time.ZonedDateTime.parse(body.oldestTimestamp as String)
      }
      if (body.latestTimestamp) {
        logParams.latestTimestamp = java.time.ZonedDateTime.parse(body.latestTimestamp as String)
      }

      def logType = eu.chargetime.ocpp.model.securityext.types.LogEnumType.valueOf(body.logType as String)
      def requestId = body.requestId as Integer

      def request = new eu.chargetime.ocpp.model.securityext.GetLogRequest(logType, requestId, logParams)
      if (body.retries) {
        request.retries = body.retries as Integer
      }
      if (body.retryInterval) {
        request.retryInterval = body.retryInterval as Integer
      }

      def confirmation = (eu.chargetime.ocpp.model.securityext.GetLogConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      logger.info("GetLog for {} — logType: {}, requestId: {}, status: {}", identifier, body.logType, requestId, confirmation?.status)
      return [chargePointId: identifier, logType: body.logType, requestId: requestId, status: confirmation?.status?.toString() ?: "Error",
              fileName: confirmation?.filename ?: null]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("GetLog error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * TC-68: Send CertificateSigned (signed cert chain) to charge point.
   *
   * curl -X POST "http://localhost:9465/ocpp16/v1/certificateSigned" \
   *   -H "Content-Type: application/json" \
   *   -d '{"identifier":"EVB-LOCAL-1","certificateChain":"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"}'
   */
  @Post("/certificateSigned")
  @Produces(MediaType.APPLICATION_JSON)
  def certificateSigned(@Body Map body) {
    String identifier = body.identifier
    if (!identifier) return [status: 'FAIL', statusText: 'Missing identifier']
    if (!body.certificateChain) return [status: 'FAIL', statusText: 'Missing certificateChain']

    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def request = new eu.chargetime.ocpp.model.securityext.CertificateSignedRequest(body.certificateChain as String)
      def confirmation = (eu.chargetime.ocpp.model.securityext.CertificateSignedConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      logger.info("CertificateSigned for {} — status: {}", identifier, confirmation?.status)
      return [chargePointId: identifier, status: confirmation?.status?.toString() ?: "Error"]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("CertificateSigned error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

  /**
   * TC-62/TC-63: Send SignedUpdateFirmware to a charge point.
   *
   * curl -X POST "http://localhost:9465/ocpp16/v1/signedUpdateFirmware" \
   *   -H "Content-Type: application/json" \
   *   -d '{"identifier":"EVB-LOCAL-1","requestId":1,"firmware":{"location":"https://example.com/firmware.bin","retrieveDateTime":"2026-04-25T00:00:00Z","signingCertificate":"-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----","signature":"base64sig=="}}'
   *
   * Optional fields: firmware.installDateTime (ISO-8601), retries, retryInterval
   */
  @Post("/signedUpdateFirmware")
  @Produces(MediaType.APPLICATION_JSON)
  def signedUpdateFirmware(@Body Map body) {
    String identifier = body.identifier
    if (!identifier) return [status: 'FAIL', statusText: 'Missing identifier']
    if (!body.requestId) return [status: 'FAIL', statusText: 'Missing requestId']
    if (!body.firmware) return [status: 'FAIL', statusText: 'Missing firmware']

    def firmwareMap = body.firmware as Map
    if (!firmwareMap.location) return [status: 'FAIL', statusText: 'Missing firmware.location']
    if (!firmwareMap.retrieveDateTime) return [status: 'FAIL', statusText: 'Missing firmware.retrieveDateTime']
    if (!firmwareMap.signingCertificate) return [status: 'FAIL', statusText: 'Missing firmware.signingCertificate']
    if (!firmwareMap.signature) return [status: 'FAIL', statusText: 'Missing firmware.signature']

    def sessionString = connections.getLatestSessionId(identifier)
    if (!sessionString) return [status: 'FAIL', statusText: 'No session found', chargePointId: identifier]

    try {
      def firmware = new eu.chargetime.ocpp.model.securityext.types.FirmwareType()
      firmware.location = firmwareMap.location as String
      firmware.retrieveDateTime = java.time.ZonedDateTime.parse(firmwareMap.retrieveDateTime as String)
      if (firmwareMap.installDateTime) {
        firmware.installDateTime = java.time.ZonedDateTime.parse(firmwareMap.installDateTime as String)
      }
      firmware.signingCertificate = firmwareMap.signingCertificate as String
      firmware.signature = firmwareMap.signature as String

      def requestId = body.requestId as Integer
      def request = new eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareRequest(requestId, firmware)
      if (body.retries) {
        request.retries = body.retries as Integer
      }
      if (body.retryInterval) {
        request.retryInterval = body.retryInterval as Integer
      }

      def confirmation = (eu.chargetime.ocpp.model.securityext.SignedUpdateFirmwareConfirmation) connections.callInline(UUID.fromString(sessionString), request)

      def confStatus = confirmation?.status?.toString() ?: "Error"
      logger.info("SignedUpdateFirmware for {} — status: {}, requestId: {}", identifier, confStatus, requestId)
      return [chargePointId: identifier, requestId: requestId, status: confStatus]
    } catch (NotConnectedException e) {
      return [status: 'FAIL', statusText: 'Offline Charger']
    } catch (Exception e) {
      logger.error("SignedUpdateFirmware error for {}: {}", identifier, e.message, e)
      return [status: 'FAIL', statusText: e.message]
    }
  }

}

