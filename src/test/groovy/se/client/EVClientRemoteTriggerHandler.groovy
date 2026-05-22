package se.client

import eu.chargetime.ocpp.feature.profile.ClientRemoteTriggerEventHandler
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageConfirmation
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageRequest
import eu.chargetime.ocpp.model.remotetrigger.TriggerMessageStatus

class EVClientRemoteTriggerHandler implements ClientRemoteTriggerEventHandler{
  @Override
  TriggerMessageConfirmation handleTriggerMessageRequest(TriggerMessageRequest request) {
    return new TriggerMessageConfirmation(TriggerMessageStatus.Accepted)
  }
}
