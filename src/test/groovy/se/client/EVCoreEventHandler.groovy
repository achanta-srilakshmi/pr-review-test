package se.client

import eu.chargetime.ocpp.feature.profile.ClientCoreEventHandler
import eu.chargetime.ocpp.model.core.ChangeAvailabilityConfirmation
import eu.chargetime.ocpp.model.core.ChangeAvailabilityRequest
import eu.chargetime.ocpp.model.core.ChangeConfigurationConfirmation
import eu.chargetime.ocpp.model.core.ChangeConfigurationRequest
import eu.chargetime.ocpp.model.core.ClearCacheConfirmation
import eu.chargetime.ocpp.model.core.ClearCacheRequest
import eu.chargetime.ocpp.model.core.DataTransferConfirmation
import eu.chargetime.ocpp.model.core.DataTransferRequest
import eu.chargetime.ocpp.model.core.GetConfigurationConfirmation
import eu.chargetime.ocpp.model.core.GetConfigurationRequest
import eu.chargetime.ocpp.model.core.RemoteStartTransactionConfirmation
import eu.chargetime.ocpp.model.core.RemoteStartTransactionRequest
import eu.chargetime.ocpp.model.core.RemoteStopTransactionConfirmation
import eu.chargetime.ocpp.model.core.RemoteStopTransactionRequest
import eu.chargetime.ocpp.model.core.ResetConfirmation
import eu.chargetime.ocpp.model.core.ResetRequest
import eu.chargetime.ocpp.model.core.UnlockConnectorConfirmation
import eu.chargetime.ocpp.model.core.UnlockConnectorRequest
import eu.chargetime.ocpp.model.core.UnlockStatus

class EVCoreEventHandler implements ClientCoreEventHandler{
  @Override
  ChangeAvailabilityConfirmation handleChangeAvailabilityRequest(ChangeAvailabilityRequest request) {
    return null
  }

  @Override
  GetConfigurationConfirmation handleGetConfigurationRequest(GetConfigurationRequest request) {
    return null
  }

  @Override
  ChangeConfigurationConfirmation handleChangeConfigurationRequest(ChangeConfigurationRequest request) {
    return null
  }

  @Override
  ClearCacheConfirmation handleClearCacheRequest(ClearCacheRequest request) {
    return null
  }

  @Override
  DataTransferConfirmation handleDataTransferRequest(DataTransferRequest request) {
    return null
  }

  @Override
  RemoteStartTransactionConfirmation handleRemoteStartTransactionRequest(RemoteStartTransactionRequest request) {
    return null
  }

  @Override
  RemoteStopTransactionConfirmation handleRemoteStopTransactionRequest(RemoteStopTransactionRequest request) {
    return null
  }

  @Override
  ResetConfirmation handleResetRequest(ResetRequest request) {
    return null
  }

  @Override
  UnlockConnectorConfirmation handleUnlockConnectorRequest(UnlockConnectorRequest request) {
    // OCPP 1.6 §6.21: return a valid confirmation — status Unlocked simulates a successful unlock.
    // Tests that need to verify UnlockFailed or NotSupported paths should override this stub.
    return new UnlockConnectorConfirmation(status: UnlockStatus.Unlocked)
  }
}
