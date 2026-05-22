package se.ocpp16

enum OCPPEventTypes {

    NEW_SESSION("NewSession"),
    INACTIVE_CHARGER("InactiveCharger"),
    LOST_SESSION("LostSession"),
    BOOT_NOTIFICATION("BootNotification"),
    METER_VALUES("MeterValues"),
    STATUS_NOTIFICATION("StatusNotification"),
    START_TRANSACTION("StartTransaction"),
    STOP_TRANSACTION("StopTransaction"),
    REMOTE_START_TRANSACTION("RemoteStartTransaction"),
    REMOTE_START_TRANSACTION_RESPONSE("RemoteStartTransactionResponse"),
    REMOTE_STOP_TRANSACTION("RemoteStopTransaction"),
    REMOTE_STOP_TRANSACTION_RESPONSE("RemoteStopTransactionResponse"),
    SET_CHARGING_PROFILE("SetChargingProfile"),
    SET_CHARGING_PROFILE_RESPONSE("SetChargingProfileResponse"),
    GET_CHARGING_PROFILE("GetChargingProfile"),
    GET_CHARGING_PROFILE_RESPONSE("GetChargingProfileResponse"),
    CLEAR_CHARGING_PROFILE("ClearChargingProfile"),
    CLEAR_CHARGING_PROFILE_RESPONSE("ClearChargingProfileResponse"),
    STATION_AUTH("StationAuth"),
    // OCPP 1.6 §3.11 ReserveNow
    RESERVE_NOW("ReserveNow"),
    RESERVE_NOW_RESPONSE("ReserveNow"),
    // OCPP 1.6 §3.3 CancelReservation
    CANCEL_RESERVATION("CancelReservation"),
    CANCEL_RESERVATION_RESPONSE("CancelReservation")

    public final String name

    private OCPPEventTypes(String name){
        this.name = name
    }

    public String toString(){
        name
    }

}