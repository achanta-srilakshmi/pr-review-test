package se


import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.websocket.WebSocketClient
import jakarta.inject.Inject
import se.kafka.TestEventListener
import se.ocpp16.OcppServer16
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest(environments = "kafka")
class ChargerControllerSpec extends Specification {

    @Shared @Inject
    EmbeddedServer embeddedServer

    @Inject
    TestEventListener topicListener

    @Shared @AutoCleanup @Inject @Client("/ocpp16/v1")
    HttpClient client

    @Shared @Inject
    OcppServer16 ocppServer

    @Inject @Client("http://localhost:9464")
    WebSocketClient wsClient

    def conditions = new PollingConditions(timeout: 10)

    def setup(){
        topicListener.getStartTransaction().clear();
    }




}
