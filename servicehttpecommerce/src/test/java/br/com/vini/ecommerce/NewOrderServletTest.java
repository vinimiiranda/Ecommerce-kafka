package br.com.vini.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Testa a validacao de entrada com um Jetty real em porta aleatoria; nao precisa de Kafka. */
class NewOrderServletTest {

    private static Server server;
    private static int port;
    private static final HttpClient client = HttpClient.newHttpClient();

    @BeforeAll
    static void startServer() throws Exception {
        server = HttpEcommerceService.createServer(0);
        server.start();
        port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @AfterAll
    static void stopServer() throws Exception {
        server.stop();
    }

    private int statusOf(String query) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/new" + query)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    @Test
    void missingParametersAreRejected() throws Exception {
        assertEquals(400, statusOf(""));
        assertEquals(400, statusOf("?email=ana@email.com"));
        assertEquals(400, statusOf("?amount=10"));
    }

    @Test
    void invalidEmailIsRejected() throws Exception {
        assertEquals(400, statusOf("?email=semarroba&amount=10"));
    }

    @Test
    void invalidOrNonPositiveAmountIsRejected() throws Exception {
        assertEquals(400, statusOf("?email=ana@email.com&amount=abc"));
        assertEquals(400, statusOf("?email=ana@email.com&amount=0"));
        assertEquals(400, statusOf("?email=ana@email.com&amount=-5"));
    }
}
