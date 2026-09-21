package br.com.vini.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpEcommerceService {

    private static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) throws Exception {
        var port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", String.valueOf(DEFAULT_PORT)));
        var server = createServer(port);
        server.start();
        server.join();
    }

    static Server createServer(int port) {
        var server = new Server(port);

        var context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");
        server.setHandler(context);
        return server;
    }
}
