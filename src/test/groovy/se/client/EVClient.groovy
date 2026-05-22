package se.client

import eu.chargetime.ocpp.JSONClient
import eu.chargetime.ocpp.feature.profile.ClientCoreProfile
import eu.chargetime.ocpp.feature.profile.ClientRemoteTriggerProfile
import jakarta.inject.Inject

class EVClient extends JSONClient {

  private JSONClient client

  public static EVClient init(String identity){
    def coreProfile = new ClientCoreProfile(new EVCoreEventHandler())
    def newClient = new EVClient(coreProfile, identity)
    def remoteProfile = new ClientRemoteTriggerProfile(new EVClientRemoteTriggerHandler())
    newClient.addFeatureProfile(remoteProfile)

    return newClient
  }

  EVClient(ClientCoreProfile coreProfile, String identity) {
    super(coreProfile, identity)
  }
}
