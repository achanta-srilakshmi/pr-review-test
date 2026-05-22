package se.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.MessageHeader
import jakarta.inject.Inject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.ocpp16.OCPPEventTypes

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingDeque

import jakarta.inject.Singleton

@Singleton
@KafkaListener(groupId = "se-test", clientId = "se-test-client")
public class TestEventListener {

    private BlockingQueue<String> newSessions = new LinkedBlockingDeque<>()
    private BlockingQueue<String> startTransactions = new LinkedBlockingDeque<>()
    private static final Logger logger = LoggerFactory.getLogger(TestEventListener.class)
    @Inject
    ObjectMapper objectMapper

    @Topic("BootCycle")
    void newSession(@KafkaKey bucketId, @MessageHeader("sessionId") String sessionId, @MessageHeader("eventType") String eventType, String payload){
        if(eventType.equalsIgnoreCase(OCPPEventTypes.NEW_SESSION.name)){
            Map requestPayload = objectMapper.readValue(payload, Map.class)
            logger.debug("In NewSession $payload")
            newSessions.add(requestPayload.get("identifier"))
        }
    }

    @Topic("StartTransaction")
    void startTransaction(@KafkaKey bucketId, @MessageHeader("sessionId") String sessionId, String payload){
        logger.debug("In StartTransaction")
        startTransactions.add(payload);
    }

    public BlockingQueue<String> getNewSession() {
        return newSessions;
    }

    public BlockingQueue<String> getStartTransaction() {
        return startTransactions;
    }
}