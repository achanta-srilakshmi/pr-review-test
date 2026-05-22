package se.ocpp16

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import eu.chargetime.ocpp.model.core.AuthorizationStatus
import eu.chargetime.ocpp.model.core.AuthorizeConfirmation
import eu.chargetime.ocpp.model.core.AuthorizeRequest
import eu.chargetime.ocpp.model.core.StartTransactionConfirmation
import eu.chargetime.ocpp.model.core.StartTransactionRequest
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import se.EdgeConfig
import se.bus.MessageProducer
import se.bus.OcppEventReceiver
import se.ocpp16.handlers.CoreProfile16EH
import se.service.ConnectionService
import se.service.IdTagCacheService
import spock.lang.Specification
import spock.lang.Subject

/**
 * Unit tests for the idTag cache gate in CoreProfile16EH.
 * Covers: TC-03-001, TC-03-002, TC-03-003, TC-03-004, TC-03-005, TC-03-006,
 *         TC-03-007, TC-03-008, TC-03-014, TC-03-016
 * TC-03-015 (StopTransaction unaffected) is not included — handleStopTransactionRequest() was not modified.
 */
class IdTagCacheGateSpec extends Specification {

    @Subject
    CoreProfile16EH handler

    ConnectionService connections = Mock()
    OcppEventReceiver ocppIncoming = Mock()
    MessageProducer messageProducer = Mock()
    IdTagCacheService idTagCacheService = Mock()
    EdgeConfig edgeConfig = new EdgeConfig()
    ObjectMapper objectMapper = new ObjectMapper()

    UUID sessionId = UUID.randomUUID()

    def setup() {
        objectMapper.registerModule(new JavaTimeModule())
        edgeConfig.kafkaNodeId = "node-test"
        edgeConfig.authBypass = false
        edgeConfig.cacheValidation = new EdgeConfig.CacheValidationConfig()

        handler = new CoreProfile16EH(
            objectMapper: objectMapper,
            ocppIncoming: ocppIncoming,
            meterRegistry: new SimpleMeterRegistry(),
            connections: connections,
            edgeConfig: edgeConfig,
            kafkaEnabled: false,           // disable Kafka to isolate gate logic
            messageProducer: messageProducer,
            idTagCacheService: idTagCacheService
        )

        connections.getChargerId(sessionId.toString()) >> "CP-001"
    }

    // -----------------------------------------------------------------------
    // TC-03-001: StartTransaction — valid idTag, idTagCheck=true → Accepted path
    // -----------------------------------------------------------------------
    def "TC-03-001: StartTransaction with valid idTag and idTagCheck=true returns Accepted"() {
        given: "idTagCheck enabled; cache loaded; idTag is valid"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-VALID") >> true

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-VALID", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then: "request proceeds — gate passes — response is Accepted"
        result.idTagInfo.status == AuthorizationStatus.Accepted
    }

    // -----------------------------------------------------------------------
    // TC-03-002: StartTransaction — invalid idTag, idTagCheck=true → Invalid, no Kafka
    // -----------------------------------------------------------------------
    def "TC-03-002: StartTransaction with invalid idTag and idTagCheck=true returns Invalid immediately"() {
        given: "idTagCheck enabled; cache loaded; idTag NOT in cache"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-UNKNOWN") >> false

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-UNKNOWN", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then: "response is Invalid — gate rejects"
        result.idTagInfo.status == AuthorizationStatus.Invalid

        and: "no Kafka message published"
        0 * ocppIncoming.ocppTransaction(_, _, _, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-03-003: StartTransaction — idTagCheck=false → proceeds normally (no gate interaction)
    // -----------------------------------------------------------------------
    def "TC-03-003: StartTransaction with idTagCheck=false bypasses gate and returns Accepted"() {
        given: "idTagCheck disabled (default)"
        edgeConfig.cacheValidation.idTagCheck = false

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-ANY", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then: "response is Accepted — gate not invoked"
        result.idTagInfo.status == AuthorizationStatus.Accepted

        and: "idTagCacheService is never consulted"
        0 * idTagCacheService.isCacheLoaded()
        0 * idTagCacheService.isValid(_)
    }

    // -----------------------------------------------------------------------
    // TC-03-004: Authorize — valid idTag, idTagCheck=true → gate passes; authBypass=true used for deterministic response
    // -----------------------------------------------------------------------
    def "TC-03-004: Authorize with valid idTag and idTagCheck=true passes the gate"() {
        given: "idTagCheck enabled; cache loaded; idTag valid; authBypass=true avoids live HTTP call to DM server"
        edgeConfig.cacheValidation.idTagCheck = true
        edgeConfig.authBypass = true     // use authBypass so we get a deterministic response without HTTP call
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-VALID") >> true

        AuthorizeRequest request = new AuthorizeRequest("RFID-VALID")

        when:
        AuthorizeConfirmation result = handler.handleAuthorizeRequest(sessionId, request)

        then: "gate passes — authBypass returns Accepted"
        result.idTagInfo.status == AuthorizationStatus.Accepted
    }

    // -----------------------------------------------------------------------
    // TC-03-005: Authorize — invalid idTag, idTagCheck=true → Immediate Invalid, no DM call
    // -----------------------------------------------------------------------
    def "TC-03-005: Authorize with invalid idTag and idTagCheck=true returns Invalid immediately — no DM call"() {
        given: "idTagCheck enabled; cache loaded; idTag NOT in cache"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-X") >> false

        AuthorizeRequest request = new AuthorizeRequest("RFID-X")

        when:
        AuthorizeConfirmation result = handler.handleAuthorizeRequest(sessionId, request)

        then: "response is Invalid — no DM Server call attempted"
        result.idTagInfo.status == AuthorizationStatus.Invalid

        and: "no Kafka authorize event published"
        0 * ocppIncoming.authorize(_, _, _)
    }

    // -----------------------------------------------------------------------
    // TC-03-006: Authorize — idTagCheck=false → gate not invoked
    // -----------------------------------------------------------------------
    def "TC-03-006: Authorize with idTagCheck=false bypasses cache gate"() {
        given: "idTagCheck disabled; authBypass enabled for deterministic result"
        edgeConfig.cacheValidation.idTagCheck = false
        edgeConfig.authBypass = true
        idTagCacheService.isCacheLoaded() >> false   // even if not loaded, gate must be skipped

        AuthorizeRequest request = new AuthorizeRequest("RFID-ANY")

        when:
        AuthorizeConfirmation result = handler.handleAuthorizeRequest(sessionId, request)

        then: "authBypass path executes — Accepted returned"
        result.idTagInfo.status == AuthorizationStatus.Accepted

        and: "idTagCacheService is never consulted"
        0 * idTagCacheService.isValid(_)
    }

    // -----------------------------------------------------------------------
    // TC-03-007: StartTransaction — empty cache + idTagCheck=true → Invalid
    // -----------------------------------------------------------------------
    def "TC-03-007: StartTransaction with empty cache and idTagCheck=true returns Invalid"() {
        given: "idTagCheck enabled; cache loaded but empty"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-ANY") >> false   // empty set, no match

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-ANY", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then:
        result.idTagInfo.status == AuthorizationStatus.Invalid
    }

    // -----------------------------------------------------------------------
    // TC-03-008: StartTransaction — cache NOT loaded + idTagCheck=true → Invalid (fail-safe)
    // -----------------------------------------------------------------------
    def "TC-03-008: StartTransaction rejects when idTagCheck=true and cache not yet loaded — fail-safe"() {
        given: "idTagCheck enabled; cache never loaded"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> false

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-ANY", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then: "fail-safe — cache not loaded, reject immediately"
        result.idTagInfo.status == AuthorizationStatus.Invalid

        and: "isValid is never called — short-circuit on cacheLoaded check"
        0 * idTagCacheService.isValid(_)
    }

    // -----------------------------------------------------------------------
    // TC-03-014: authBypass=true unaffected by idTag cache — Authorize
    // -----------------------------------------------------------------------
    def "TC-03-014: authBypass=true with idTagCheck=false is unaffected by idTag cache"() {
        given: "authBypass=true; idTagCheck=false (default)"
        edgeConfig.authBypass = true
        edgeConfig.cacheValidation.idTagCheck = false

        AuthorizeRequest request = new AuthorizeRequest("RFID-ANY")

        when:
        AuthorizeConfirmation result = handler.handleAuthorizeRequest(sessionId, request)

        then: "authBypass path executes unchanged"
        result.idTagInfo.status == AuthorizationStatus.Accepted

        and: "no idTagCacheService interaction"
        0 * idTagCacheService._
    }

    // -----------------------------------------------------------------------
    // TC-03-016: StartTransaction response always contains transactionId (OCPP 1.6 §3.22)
    // -----------------------------------------------------------------------
    def "TC-03-016: StartTransaction rejected by idTag gate still returns non-zero transactionId"() {
        given: "idTagCheck enabled; idTag not valid"
        edgeConfig.cacheValidation.idTagCheck = true
        idTagCacheService.isCacheLoaded() >> true
        idTagCacheService.isValid("RFID-X") >> false

        StartTransactionRequest request = new StartTransactionRequest(1, "RFID-X", 0, null)

        when:
        StartTransactionConfirmation result = handler.handleStartTransactionRequest(sessionId, request)

        then: "OCPP 1.6 §3.22: transactionId MUST be present even when status=Invalid"
        result.transactionId > 0
        result.idTagInfo.status == AuthorizationStatus.Invalid
    }
}
