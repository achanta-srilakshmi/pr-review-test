package se.ocpp16.handlers

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.feature.profile.ServerFirmwareManagementEventHandler
import eu.chargetime.ocpp.model.firmware.DiagnosticsStatusNotificationConfirmation
import eu.chargetime.ocpp.model.firmware.DiagnosticsStatusNotificationRequest
import eu.chargetime.ocpp.model.firmware.FirmwareStatusNotificationConfirmation
import eu.chargetime.ocpp.model.firmware.FirmwareStatusNotificationRequest
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.service.ConnectionService

@Singleton
class FirmwareEH implements ServerFirmwareManagementEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(FirmwareEH.class)

  @Inject
  EdgeConfig edgeConfig

  @Inject
  OcppEventReceiver ocppIncoming

  @Inject
  ObjectMapper objectMapper

  @Inject
  MeterRegistry meterRegistry

  @Inject
  ConnectionService connections

  @Inject
  MessageProducer messageProducer

  boolean kafkaEnabled = true

  @Override
  DiagnosticsStatusNotificationConfirmation handleDiagnosticsStatusNotificationRequest(UUID sessionId, DiagnosticsStatusNotificationRequest request) {
    def identifier = connections.getChargerId(sessionId.toString())
    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    ocppIncoming.diaglosticsLogStatus(edgeConfig.kafkaNodeId, identifier, objectMapper.writeValueAsString(request))

    def response = new DiagnosticsStatusNotificationConfirmation()

    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  @Override
  FirmwareStatusNotificationConfirmation handleFirmwareStatusNotificationRequest(UUID sessionId, FirmwareStatusNotificationRequest request) {
    def identifier = connections.getChargerId(sessionId.toString())
    Integer messageTypeId = 2
    String uniqueId = System.currentTimeMillis().toString()
    ProtocolLog.info(true, identifier, messageTypeId, uniqueId, request?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(request))
    meterRegistry.counter("se.in", "type", request.class.simpleName).increment()
    publishMetricsCounterIncrement("se.in", ["type": request.class.simpleName], 1)

    ocppIncoming.firmwareStatus(edgeConfig.kafkaNodeId, identifier, objectMapper.writeValueAsString(request))

    def response = new FirmwareStatusNotificationConfirmation()

    messageTypeId = 3
    ProtocolLog.info(false, identifier, messageTypeId, uniqueId, response?.getClass()?.getSimpleName(), objectMapper.writeValueAsString(response))

    return response
  }

  private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
    def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
    if(kafkaEnabled)
      messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
  }
}
