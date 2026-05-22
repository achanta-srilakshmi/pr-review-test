package se.ocpp16.handlers

import eu.chargetime.ocpp.PromiseFulfiller
import eu.chargetime.ocpp.SessionEvents
import eu.chargetime.ocpp.feature.profile.ServerCoreEventHandler
import eu.chargetime.ocpp.model.Confirmation
import eu.chargetime.ocpp.model.Request
import eu.chargetime.ocpp.model.core.*
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.service.ConnectionService

import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture

/**
 * Implementation class for Core Profile of OCPP that only writes the events to Log. 
 * This is one of the implementations of the protocol that can be used for debugging purposes.
 */
@Singleton
public class SoloEventHandler implements ServerCoreEventHandler, PromiseFulfiller {
  private static final Logger logger = LoggerFactory.getLogger(SoloEventHandler.class);

  @Inject
  ConnectionService connections

  @Override
  public AuthorizeConfirmation handleAuthorizeRequest(UUID sessionIndex, AuthorizeRequest request) {
    logger.info("{} sent a request to authorize with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())
    return new AuthorizeConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Accepted))
  }

  @Override
  public BootNotificationConfirmation handleBootNotificationRequest(UUID sessionIndex, BootNotificationRequest request) {
    logger.info("{} sent a boot request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())
    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())
    BootNotificationConfirmation conf = new BootNotificationConfirmation(currentTime: timeNow, interval: 180, status: RegistrationStatus.Accepted)

    return conf
  }

  @Override
  public DataTransferConfirmation handleDataTransferRequest(UUID sessionIndex, DataTransferRequest request) {
    logger.info("{} sent a data transfer request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())

    return new DataTransferConfirmation()
  }

  @Override
  public HeartbeatConfirmation handleHeartbeatRequest(UUID sessionIndex, HeartbeatRequest request) {
    logger.info("{} sent a heartbeat request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())
    ZonedDateTime timeNow = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())
    return new HeartbeatConfirmation(currentTime: timeNow)
  }

  @Override
  public MeterValuesConfirmation handleMeterValuesRequest(UUID sessionIndex, MeterValuesRequest request) {
    logger.info("{} sent a meter value request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())

    return new MeterValuesConfirmation()
  }

  @Override
  public StartTransactionConfirmation handleStartTransactionRequest(UUID sessionIndex, StartTransactionRequest request) {
    logger.info("{} sent a start transaction request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())

    return new StartTransactionConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Accepted), transactionId: System.currentTimeMillis() % (1000000000))
  }

  @Override
  public StatusNotificationConfirmation handleStatusNotificationRequest(UUID sessionIndex, StatusNotificationRequest request) {
    logger.info("{} sent a status notification request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())

    return new StatusNotificationConfirmation()
  }

  @Override
  public StopTransactionConfirmation handleStopTransactionRequest(UUID sessionIndex, StopTransactionRequest request) {
    logger.info("{} sent a stop transaction request with payload as {}",
        connections.getChargerId(sessionIndex.toString()),
        request.toString())

    return new StopTransactionConfirmation(idTagInfo: new IdTagInfo(status: AuthorizationStatus.Accepted))
  }

  @Override
  void fulfill(CompletableFuture<Confirmation> promise, SessionEvents eventHandler, Request request) {
    logger.info(request.toString())
  }
}

