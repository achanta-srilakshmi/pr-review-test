package se

import io.micronaut.http.HttpRequest
import io.micronaut.websocket.WebSocketSession
import io.micronaut.websocket.annotation.ClientWebSocket
import io.micronaut.websocket.annotation.OnClose
import io.micronaut.websocket.annotation.OnMessage
import io.micronaut.websocket.annotation.OnOpen
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Future

@ClientWebSocket("/{identifier}")
public abstract class TestWebSocketClient implements AutoCloseable {

    private WebSocketSession session
    private HttpRequest request
    private String identifier
    private Collection<String> replies = new ConcurrentLinkedQueue<>()

    private static final Logger logger = LoggerFactory.getLogger(TestWebSocketClient.class)

    @OnOpen
    void onOpen(String identifier, WebSocketSession session, HttpRequest request) {
        logger.debug("Opened websocket for ${identifier}")
        this.identifier = identifier
        this.session = session
        this.request = request
    }

    String getIdentifier() {
        identifier
    }

    Collection<String> getReplies() {
        replies
    }

    WebSocketSession getSession() {
        session
    }

    HttpRequest getRequest() {
        request
    }

    @OnMessage
    void onMessage(String message) {
        logger.debug("Incoming message for $identifier: $message" )
        replies.add(message)
    }

    @OnClose
    void close() throws Exception {
        println "Websocket closing connection on $identifier"
    }
}