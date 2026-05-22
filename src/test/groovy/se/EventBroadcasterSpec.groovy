package se

import io.micronaut.context.annotation.Property
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.websocket.WebSocketClient
import io.netty.util.internal.ThreadLocalRandom
import jakarta.inject.Inject
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import se.kafka.TestEventListener
import se.ocpp16.OcppServer16
import spock.lang.AutoCleanup
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.TimeUnit

@MicronautTest(environments = "kafka")
class EventBroadcasterSpec extends Specification {

    @Shared @Inject
    EmbeddedServer embeddedServer

    @Inject
    TestEventListener topicListener

    @Shared @Inject
    OcppServer16 ocppServer

    @Property(name = "se.ports.webSocket")
    String port

    @Shared @AutoCleanup @Inject
    WebSocketClient wsClient

    def conditions = new PollingConditions(timeout: 12)

    @Ignore
    void "scratch test method" () {
        given:
        def limit = 20
        def lam = { cap ->
            int sum = 0
            (0..cap).each({ sum = sum + it })
            println sum
            return sum
        }
        def myCallable = Mono.fromCallable(() -> lam(limit))
        def result = myCallable.subscribeOn((Schedulers.immediate()))

        //def blockedResult = result.block()
//        raw.subscribe()

        expect:
        conditions.within(4) {
            assert result.single().then(Mono.just()) == 210
        }

    }

    void "Expect a Message on Kafka on the topic 'BootCycle' for every New Websocket Connection - variant 1"() {
        setup:
        topicListener.getNewSession().clear();

        when:
        def identifier = 'EVK-'+ ThreadLocalRandom.current().nextInt(100000, 999999)
        wsClient.connect(TestWebSocketClient.class, "http://localhost:${port}"+"/"+identifier).subscribe()

        then:
        conditions.within(12) {
            def payload = topicListener.getNewSession().poll(12, TimeUnit.SECONDS)
            assert payload == identifier
        }
    }

}
