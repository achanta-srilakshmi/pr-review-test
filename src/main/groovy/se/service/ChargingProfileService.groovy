package se.service

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import eu.chargetime.ocpp.NotConnectedException
import eu.chargetime.ocpp.model.core.ChargingProfile
import eu.chargetime.ocpp.model.core.ChargingProfileKindType
import eu.chargetime.ocpp.model.core.ChargingProfilePurposeType
import eu.chargetime.ocpp.model.core.ChargingRateUnitType
import eu.chargetime.ocpp.model.core.ChargingSchedule
import eu.chargetime.ocpp.model.core.ChargingSchedulePeriod
import eu.chargetime.ocpp.model.core.RecurrencyKindType
import eu.chargetime.ocpp.model.smartcharging.ChargingProfileStatus
import eu.chargetime.ocpp.model.smartcharging.ClearChargingProfileConfirmation
import eu.chargetime.ocpp.model.smartcharging.ClearChargingProfileRequest
import eu.chargetime.ocpp.model.smartcharging.GetCompositeScheduleConfirmation
import eu.chargetime.ocpp.model.smartcharging.GetCompositeScheduleRequest
import eu.chargetime.ocpp.model.smartcharging.GetCompositeScheduleStatus
import eu.chargetime.ocpp.model.smartcharging.SetChargingProfileConfirmation
import eu.chargetime.ocpp.model.smartcharging.SetChargingProfileRequest
import io.micrometer.core.instrument.MeterRegistry
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.OCPPEventTypes
import se.ocpp16.handlers.ProtocolLog

import java.time.ZoneId
import java.time.ZonedDateTime

@Singleton
class ChargingProfileService {

    private static final Logger logger = LoggerFactory.getLogger(ChargingProfileService.class)

    @Inject
    MessageProducer messageProducer

    @Inject
    ObjectMapper objectMapper

    @Inject
    EdgeConfig edgeConfig

    @Inject
    ConnectionService connections

    @Inject
    MeterRegistry meterRegistry

    @Inject
    OcppEventReceiver incoming

    boolean kafkaEnabled = true

    def setChargingProfile(String identifier, String payload) {
        try {

            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId){
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }

            logger.debug("Triggering Charge Profile - ${payload} for identifier ${identifier}")

            ChargingProfileRequest requestPayload = objectMapper.readValue(payload, ChargingProfileRequest.class)

            if (requestPayload) {
                ChargingProfile chargingProfile = prepareChargingProfile(requestPayload)
                SetChargingProfileRequest request = new SetChargingProfileRequest(0, chargingProfile)
                def chargingProfileConfirmation
                request.connectorId=requestPayload.connectorIndex
                try {
                    chargingProfileConfirmation = (SetChargingProfileConfirmation) connections.callInline(UUID.fromString(sessionId), request)
                    if (chargingProfileConfirmation?.status != ChargingProfileStatus.Accepted) {
                        def errorPayload = chargingProfileConfirmation?.properties ?: [:]
                        errorPayload["connectorId"] = requestPayload.connectorIndex
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, chargingProfile.chargingProfilePurpose.toString() + "Error", objectMapper.writeValueAsString(chargingProfileConfirmation ?: [:]))
                    }
                    incoming.chargingProfile(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.SET_CHARGING_PROFILE_RESPONSE.toString(),
                             objectMapper.writeValueAsString(["status": chargingProfileConfirmation?.status?.toString(), transactionId: requestPayload.transactionId, chargingProfileId: requestPayload.chargingProfileId, connectorId: requestPayload.connectorIndex]))
                } catch (NotConnectedException nce) {
                    //NCE is reported at trace level
                    logger.trace("Probable wrong instance")
                } catch (Exception e) {
                    logger.error("Error setting charging profile " + e)
                    def errorPayload = chargingProfileConfirmation?.properties ?: [:]
                    errorPayload["connectorId"] = requestPayload.get("connectorIndex")
                    incoming.chargerError(edgeConfig.kafkaNodeId, identifier, ChargingProfilePurposeType.ChargePointMaxProfile.toString() + "Error", objectMapper.writeValueAsString(chargingProfileConfirmation ?: [:]))
                }
            }

        } catch (Exception e) {
            logger.error("Error setting charging profile " + e)
        }
    }

    def clearChargingProfile(String identifier,String payload){

        def sessionId = connections.getLatestSessionId(identifier)
        if (!sessionId){
            meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
            publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
            return
        }

        def clearChargingProfileRequest = objectMapper.readValue(payload, Map.class)
        logger.debug("Requesting to clear charge profile for session Id ${sessionId}")
        def clearChargingProfileConfirmation
        try {
            ClearChargingProfileRequest request = new ClearChargingProfileRequest()
            request.connectorId = 0
            if (clearChargingProfileRequest.chargingProfileId)
                request.id = clearChargingProfileRequest.chargingProfileId as Integer
            if (clearChargingProfileRequest.connectorIndex)
                request.connectorId = clearChargingProfileRequest.connectorIndex as Integer
            if (clearChargingProfileRequest.chargingProfilePurpose)
                request.chargingProfilePurpose = clearChargingProfileRequest.chargingProfilePurpose as ChargingProfilePurposeType
            if (clearChargingProfileRequest.stackLevel)
                request.stackLevel = clearChargingProfileRequest.stackLevel as Integer

            logger.info("clearChargingProfile validation: " + request.validate());
            clearChargingProfileConfirmation = (ClearChargingProfileConfirmation) connections.callInline(UUID.fromString(sessionId), request)
            if (clearChargingProfileConfirmation?.status != ChargingProfileStatus.Accepted) {
                def errorPayload = clearChargingProfileConfirmation?.properties ?: [:]
                errorPayload["connectorId"] = clearChargingProfileRequest.get("connectorIndex")
                incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "ClearChargingProfileError", objectMapper.writeValueAsString(clearChargingProfileConfirmation ?: [:]))
            }
            incoming.chargingProfile(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.CLEAR_CHARGING_PROFILE_RESPONSE.toString(),
                    objectMapper.writeValueAsString(["status": clearChargingProfileConfirmation?.status?.toString(), chargingProfileId: clearChargingProfileRequest.chargingProfileId, connectorId: clearChargingProfileRequest.connectorIndex]))
        } catch (NotConnectedException nce) {
            //NCE is reported at trace level
            logger.trace("Probable wrong instance")
        } catch (Exception e) {
            logger.error("Error clearing charging profile " + e)
            def errorPayload = clearChargingProfileConfirmation?.properties ?: [:]
            errorPayload["connectorId"] = clearChargingProfileRequest.get("connectorId")
            incoming.chargerError(edgeConfig.kafkaNodeId, identifier, "ClearChargingProfileError", objectMapper.writeValueAsString(clearChargingProfileConfirmation ?: [:]))
        }
    }

    def getCompositeSchedule(String identifier, String payload){
        try {

            def sessionId = connections.getLatestSessionId(identifier)
            if (!sessionId){
                meterRegistry.counter("se.sessionmap.nomatch", "identifier", identifier).increment()
                publishMetricsCounterIncrement("se.sessionmap.nomatch", ["identifier": identifier], 1)
                return
            }

            logger.debug("Triggering get Charge Profile - ${payload} for identifier ${identifier}")
            Map requestPayload = objectMapper.readValue(payload, Map.class)

            if (requestPayload) {
                GetCompositeScheduleRequest request = new GetCompositeScheduleRequest(requestPayload.connectorIndex as Integer, requestPayload.duration as Integer)
                def getCompositeScheduleConfirmation
                request.connectorId=requestPayload.connectorIndex
                try {
                    getCompositeScheduleConfirmation = (GetCompositeScheduleConfirmation) connections.callInline(UUID.fromString(sessionId), request)
                    if (getCompositeScheduleConfirmation?.status != GetCompositeScheduleStatus.Accepted) {
                        def errorPayload = getCompositeScheduleConfirmation?.properties ?: [:]
                        errorPayload["connectorId"] = requestPayload.get("connectorIndex")
                        incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.GET_CHARGING_PROFILE_RESPONSE.toString() + "Error", objectMapper.writeValueAsString(errorPayload))
                    }
                    incoming.chargingProfile(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.GET_CHARGING_PROFILE_RESPONSE.toString(),
                            objectMapper.writeValueAsString(["status": getCompositeScheduleConfirmation?.status?.toString(), compositeSchedule: getCompositeScheduleConfirmation ]))

                } catch (NotConnectedException nce) {
                    //NCE is reported at trace level
                    logger.trace("Probable wrong instance")
                } catch (Exception e) {
                    logger.error("Error setting charging profile " + e)
                    def errorPayload = getCompositeScheduleConfirmation?.properties ?: [:]
                    errorPayload["connectorId"] = requestPayload.get("connectorIndex")
                    incoming.chargerError(edgeConfig.kafkaNodeId, identifier, OCPPEventTypes.GET_CHARGING_PROFILE_RESPONSE.toString() + "Error", objectMapper.writeValueAsString(errorPayload))
                }
            }
        } catch (Exception e) {
            logger.error("Error setting charging profile " + e)
        }

    }

    /**
     * Public entry point for building a ChargingProfile from a ChargingProfileRequest.
     * Used by OcppEventBroadcaster for RemoteStartTransaction with embedded TxProfile.
     * US-RCP102-01: Remote Start Transaction with Charging Profile
     *
     * @param chargingProfileRequest The deserialized ChargingProfileRequest
     * @return ChargingProfile OCPP model object
     */
    ChargingProfile buildChargingProfile(ChargingProfileRequest chargingProfileRequest) {
        return prepareChargingProfile(chargingProfileRequest)
    }

    private ChargingProfile prepareChargingProfile(ChargingProfileRequest chargingProfileCmd) {
            ChargingProfile chargingProfile = new ChargingProfile()
            chargingProfile.chargingProfileId = chargingProfileCmd.chargingProfileId?chargingProfileCmd.chargingProfileId as Integer:(Math.random() * 100000).intValue()
            chargingProfile.transactionId = chargingProfileCmd.transactionId
            chargingProfile.chargingProfilePurpose = chargingProfileCmd.chargingProfilePurposeType
            chargingProfile.stackLevel = chargingProfileCmd.stackLevel?chargingProfileCmd.stackLevel:1
            chargingProfile.recurrencyKind=chargingProfileCmd.recurrencyKind

            if(chargingProfileCmd.recurrencyKind.toString() == RecurrencyKindType.Daily.toString() || chargingProfileCmd.recurrencyKind.toString() == RecurrencyKindType.Weekly.toString())
                chargingProfile.chargingProfileKind = ChargingProfileKindType.Recurring
            else
                chargingProfile.chargingProfileKind = ChargingProfileKindType.Absolute

            if (chargingProfileCmd.valid_from)
                chargingProfile.validFrom = ZonedDateTime.parse(chargingProfileCmd.valid_from)
            if (chargingProfileCmd.valid_to)
                chargingProfile.validTo = ZonedDateTime.parse(chargingProfileCmd.valid_to)

            ChargingSchedule chargingSchedule = new ChargingSchedule()
            chargingSchedule.duration = chargingProfileCmd.duration
            if(chargingProfileCmd.start_date_time)
                chargingSchedule.startSchedule = ZonedDateTime.parse(chargingProfileCmd.start_date_time)
            chargingSchedule.chargingRateUnit = chargingProfileCmd.charging_rate_unit
            chargingSchedule.minChargingRate = chargingProfileCmd.min_charging_rate
            chargingSchedule.chargingSchedulePeriod = new ChargingSchedulePeriod[chargingProfileCmd.charging_profile_period?.size()]
            chargingProfileCmd.charging_profile_period.eachWithIndex{ it, index ->
                chargingSchedule.chargingSchedulePeriod[index] = new ChargingSchedulePeriod(startPeriod: it.start_period, limit: it.limit)
            }

            chargingProfile.chargingSchedule = chargingSchedule

            return chargingProfile

        /*ZonedDateTime startSchedule = ZonedDateTime.parse(chargingProfileCmd.validFrom)
        if (chargingProfileCmd.startSchedule)
            startSchedule = ZonedDateTime.parse(chargingProfileCmd.startSchedule)

        Integer mins = startSchedule.getMinute()
        Integer hours = startSchedule.getHour()
        Integer sec = startSchedule.getSecond()
        Integer scheduleStart = (hours * 3600) + (mins * 60) + sec

        ChargingSchedulePeriod[] periods = new ChargingSchedulePeriod[1]
        periods[0] = new ChargingSchedulePeriod(scheduleStart, chargingProfileCmd.limit)

        startSchedule = startSchedule.minusSeconds(sec)
        startSchedule = startSchedule.minusMinutes(mins)
        startSchedule = startSchedule.minusHours(hours)

        ChargingRateUnitType chargingRateUnit = ChargingRateUnitType.W
        if (chargingProfileCmd.chargingRateUnit)
            chargingRateUnit = chargingProfileCmd.chargingRateUnit

        ChargingSchedule chargingSchedule = new ChargingSchedule(chargingRateUnit, periods)
        if(chargingProfileCmd.duration !=null)
            chargingSchedule.duration = scheduleStart + Integer.valueOf(chargingProfileCmd.duration)
        chargingSchedule.startSchedule = startSchedule

        logger.info("charging Schedule chargingRateUnit: " + chargingSchedule.chargingRateUnit)
        if (chargingProfileCmd.numberPhases && chargingProfileCmd.numberPhases == 1)
            periods[0].numberPhases = chargingProfileCmd.numberPhases
        chargingSchedule.chargingSchedulePeriod = periods

        ChargingProfile chargingProfiles = new ChargingProfile(
                chargingProfileCmd.chargingProfileId,
                1,
                chargingProfilePurposeType,
                ChargingProfileKindType.Absolute,
                chargingSchedule
        )

        if (chargingProfileCmd.chargingProfileId)
            chargingProfiles.chargingProfileId = chargingProfileCmd.chargingProfileId
        else
            chargingProfiles.chargingProfileId = (Math.random() * 100000).intValue()
        chargingProfiles.stackLevel = 1
        chargingProfiles.chargingProfilePurpose = chargingProfilePurposeType

        if (chargingProfileCmd.chargingProfileKindType == ChargingProfileKindType.Recurring.toString()) {
            chargingProfiles.chargingProfileKind = ChargingProfileKindType.Recurring
            chargingProfiles.recurrencyKind = RecurrencyKindType.Daily

            if (chargingProfileCmd.recurrencyKindType == RecurrencyKindType.Weekly.toString())
                chargingProfiles.recurrencyKind = RecurrencyKindType.Weekly
        }

        chargingProfiles.validFrom = ZonedDateTime.ofInstant(Calendar.getInstance().toInstant(), ZoneId.systemDefault())

        java.util.GregorianCalendar
        if (chargingProfileCmd.validFrom)
            chargingProfiles.validFrom = ZonedDateTime.parse(chargingProfileCmd.validFrom)
        if (chargingProfileCmd.validTo)
            chargingProfiles.validTo = ZonedDateTime.parse(chargingProfileCmd.validTo)
        if(chargingProfileCmd.transactionId)
            chargingProfiles.transactionId = chargingProfileCmd.transactionId

        return chargingProfiles*/
    }

    private void publishMetricsCounterIncrement(String metricName, Map labels, int value) {
        def payload = objectMapper.writeValueAsString([metric: metricName, labels: labels, value: value])
        if(kafkaEnabled)
            messageProducer.publishMetricsCounterIncrement(edgeConfig.kafkaNodeId, payload)
    }
}

class ChargingProfileRequest{
    String start_date_time
    Integer duration
    ChargingRateUnitType charging_rate_unit
    Double min_charging_rate
    List<ChargingProfilePeriod> charging_profile_period
    String valid_from
    String valid_to
    RecurrencyKindType recurrencyKind
    
    private ChargingProfilePurposeType chargingProfilePurposeType
    Integer transactionId
    Integer chargingProfileId
    Integer connectorIndex = 0
    Integer stackLevel
    
    // Getter for chargingProfilePurposeType
    ChargingProfilePurposeType getChargingProfilePurposeType() {
        return chargingProfilePurposeType
    }
    
    // Custom setter to handle both string and enum values, and alternative field names
    @JsonProperty("chargingProfilePurposeType")
    void setChargingProfilePurposeType(Object value) {
        if (value == null) {
            this.chargingProfilePurposeType = null
        } else if (value instanceof ChargingProfilePurposeType) {
            this.chargingProfilePurposeType = value
        } else if (value instanceof String) {
            try {
                // Try exact match first
                this.chargingProfilePurposeType = ChargingProfilePurposeType.valueOf(value.toString())
            } catch (IllegalArgumentException e) {
                try {
                    // Try case-insensitive match
                    this.chargingProfilePurposeType = ChargingProfilePurposeType.values().find {
                        it.name().equalsIgnoreCase(value.toString())
                    }
                    if (this.chargingProfilePurposeType == null) {
                        throw new IllegalArgumentException("No matching enum value found")
                    }
                } catch (Exception ex) {
                    throw new IllegalArgumentException("Invalid chargingProfilePurposeType: ${value}. Must be one of: ${ChargingProfilePurposeType.values()*.name().join(', ')}", ex)
                }
            }
        } else {
            throw new IllegalArgumentException("chargingProfilePurposeType must be a String or ChargingProfilePurposeType enum, got: ${value.class.simpleName}")
        }
    }
    
    // Alternative setter for "chargingProfilePurpose" (without "Type" suffix)
    @JsonProperty("chargingProfilePurpose")
    void setChargingProfilePurpose(Object value) {
        setChargingProfilePurposeType(value)
    }
}

class ChargingProfilePeriod{
    Integer start_period
    Double limit
}
