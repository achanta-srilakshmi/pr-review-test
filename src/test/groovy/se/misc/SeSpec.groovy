package se.misc


import eu.chargetime.ocpp.JSONClient
import io.micronaut.runtime.EmbeddedApplication
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import se.client.EVClient
import se.client.EVClientEvents
import spock.lang.Shared
import spock.lang.Specification
import jakarta.inject.Inject
import se.ocpp16.OcppServer16
import spock.util.concurrent.BlockingVariable
import spock.util.concurrent.PollingConditions

@MicronautTest
class SeSpec extends Specification {

    @Inject
    EmbeddedApplication<?> application
    //EmbeddedServer embeddedServer = ApplicationContext.run(EmbeddedServer, ['coreEventHandler': new LoggerEH(), 'coreEvents': new LogServerEvents()])

    @Inject
    @Shared
    OcppServer16 server

    @Shared
    JSONClient jsonClient

    def setupSpec() {

    }

    void 'app is running'() {
        given:
        def conditions = new PollingConditions(initialDelay: 1, timeout: 30, factor: 2)
        expect:
        application.running
        and:
        conditions.eventually {
            !server.jsonServer.closed
        }
    }

    void 'client can connect'() {
        given:
        def timer = new BlockingVariable<Boolean>(15.0)
        def conditions = new PollingConditions(initialDelay: 1, timeout: 30, factor: 2)
        jsonClient = EVClient.init("TEST-ABC")
        expect:
        conditions.eventually {
            !server.jsonServer.closed
        }

        when:
        jsonClient.connect("ws://localhost:9464", new EVClientEvents())
        then:
        conditions.eventually {
            assert jsonClient.sessionId != null
            assert jsonClient.sessionId != ''
            assert server.jsonServer.server.sessions.size() == 1
        }
    }


}
