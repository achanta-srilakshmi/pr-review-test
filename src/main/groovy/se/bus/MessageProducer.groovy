package se.bus

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic

@KafkaClient
interface MessageProducer {
    @Topic("metrics-counter-increment")
    void publishMetricsCounterIncrement(@KafkaKey String bucketId, String payload)

    @Topic("metrics-gauge-update")
    void publishMetricsGaugeUpdate(@KafkaKey String bucketId, String payload)

      @Topic("metrics-gauge-update")
    void publishMetricsGaugeUpdate(@KafkaKey String bucketId, String payload)
}

