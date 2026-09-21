package br.com.vini.ecommerce;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    /** Quem chama a API nao pode ficar minutos esperando: se o Kafka nao confirma em 10s, responde 503. */
    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>(PUBLISH_TIMEOUT);
    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>(PUBLISH_TIMEOUT);

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        var email = req.getParameter("email");
        if (email == null || !email.contains("@")) {
            reply(resp, HttpServletResponse.SC_BAD_REQUEST, "Parametro 'email' obrigatorio e deve ser um e-mail valido");
            return;
        }

        var amountParam = req.getParameter("amount");
        BigDecimal amount;
        try {
            amount = amountParam == null ? null : new BigDecimal(amountParam);
        } catch (NumberFormatException e) {
            amount = null;
        }
        if (amount == null) {
            reply(resp, HttpServletResponse.SC_BAD_REQUEST, "Parametro 'amount' obrigatorio e deve ser um numero");
            return;
        }
        if (amount.signum() <= 0) {
            reply(resp, HttpServletResponse.SC_BAD_REQUEST, "Parametro 'amount' deve ser maior que zero");
            return;
        }

        try {
            var orderId = UUID.randomUUID().toString();
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new Order(orderId, amount, email));

            var emailCode = "Thank you for order! We are processing your order!";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
        } catch (ExecutionException e) {
            // Ex.: brokers fora do ar ou replicas insuficientes (min.insync.replicas). O pedido NAO foi aceito.
            log("Falha ao publicar o pedido no Kafka", e);
            reply(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Nao foi possivel registrar o pedido agora. Tente novamente.");
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reply(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Requisicao interrompida");
            return;
        }

        System.out.println("Processo da nova compra terminado!");
        reply(resp, HttpServletResponse.SC_OK, "Processo da nova compra terminado!");
    }

    private static void reply(HttpServletResponse resp, int status, String message) throws IOException {
        resp.setStatus(status);
        resp.setContentType("text/plain;charset=UTF-8");
        resp.getWriter().println(message);
    }
}
