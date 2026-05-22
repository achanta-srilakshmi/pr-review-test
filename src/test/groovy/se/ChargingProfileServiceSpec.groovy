package se

import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.model.core.ChargingProfile
import eu.chargetime.ocpp.model.core.ChargingProfileKindType
import eu.chargetime.ocpp.model.core.ChargingProfilePurposeType
import eu.chargetime.ocpp.model.core.ChargingRateUnitType
import eu.chargetime.ocpp.model.core.RecurrencyKindType
import eu.chargetime.ocpp.model.smartcharging.ChargingProfileStatus
import eu.chargetime.ocpp.model.smartcharging.SetChargingProfileConfirmation
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import se.bus.OcppEventReceiver
import se.kafka.TestEventListener
import se.ocpp16.OCPPEventTypes
import se.service.ChargingProfileService
import se.service.ConnectionService
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Instant
import java.time.ZonedDateTime
import java.util.concurrent.TimeUnit

@MicronautTest
class ChargingProfileServiceSpec extends Specification {


    @Inject
    ChargingProfileService chargingProfileService
    @Inject
    ObjectMapper objectMapper

    ConnectionService connections
    OcppEventReceiver incoming


    public void setup(){
        connections = Mock(ConnectionService.class)
    }
    void "validate set charging profile method"(){

        given:
        long transactionId = 660863993
        String profileType = "TxProfile"


        def payload = [
            "chargingProfileId":25361405,
            "connectorIndex":1,
            "chargingProfilePurposeType":"TxProfile",
            "chargingRateUnit": "W",
            "duration": 3600,
            "limit": 16,
            "validFrom": "2023-03-01T00:00:00Z",
            "startSchedule": "2023-03-01T00:00:00Z",
            "transactionId": transactionId,
            "charging_profile_period":[
                    [
                            "start_period":0,
                            "limit":20.0
                    ],
                    [
                            "start_period":1000,
                            "limit":30.0
                    ]
            ]
        ]
        SetChargingProfileConfirmation confirmation = new SetChargingProfileConfirmation(ChargingProfileStatus.Accepted)
        String identifier="test-1"
        def connections = Mock(ConnectionService)
        def incoming = Stub(OcppEventReceiver)
        chargingProfileService.connections = connections
        chargingProfileService.incoming = incoming
        connections.getLatestSessionId(identifier) >> "1a2c7940-3f25-4d43-ac17-b6294420fea8"
        when:
        chargingProfileService.setChargingProfile(identifier,objectMapper.writeValueAsString(payload))

        then:
        1 * connections.callInline(UUID.fromString("1a2c7940-3f25-4d43-ac17-b6294420fea8"), _) >> confirmation

    }

    void "validate set charging profile method TxDefaultProfile"(){

        given:
        long transactionId = 660863993
        String profileType = "TxDefaultProfile"
        def chargingProfilePayload = [
                "chargingProfileId"        : (Math.random() * 100000).intValue(),
                "connectorIndex"           : 0,
                "chargingProfilePurposeType": "TxDefaultProfile",
                "recurrencyKind"           : "Daily",
                "stackLevel"               : 1,
                "duration"                 : 3600,
                "charging_rate_unit"       : "W",
                "charging_profile_period"  : [
                        ["start_period": 0, "limit": 16.0]
                ]
        ]
        /*def payload = [
            "chargingRateUnit": "A",
            "duration": 24000,
            "limit": 16,
            "validFrom": "2023-03-02T00:00:00Z",
            "startSchedule": "2023-03-02T00:00:00Z",
            "chargingProfileKindType": "Recurring",
            "recurrencyKindType": "Daily"
        ]*/
        SetChargingProfileConfirmation confirmation = new SetChargingProfileConfirmation(ChargingProfileStatus.Accepted)
        String identifier="test-1"
        def connections = Mock(ConnectionService)
        def incoming = Stub(OcppEventReceiver)
        chargingProfileService.connections = connections
        chargingProfileService.incoming = incoming
        connections.getLatestSessionId(identifier) >> "1a2c7940-3f25-4d43-ac17-b6294420fea8"
        when:
        chargingProfileService.setChargingProfile(identifier,objectMapper.writeValueAsString(chargingProfilePayload))

        then:
        1 * connections.callInline(UUID.fromString("1a2c7940-3f25-4d43-ac17-b6294420fea8"), _) >> confirmation
    }

}
