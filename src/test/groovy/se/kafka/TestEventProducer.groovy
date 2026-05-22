package se.kafka

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.annotation.MessageHeader

@KafkaClient
public interface TestEventProducer {
    @Topic("NewSession")
    void newSession(@KafkaKey bucketId, @MessageHeader("sessionId") String sessionId, String identifier);
}
