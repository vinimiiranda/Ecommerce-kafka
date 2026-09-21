package br.com.vini.ecommerce;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) throws Exception {
        try (var orderDispatcher = new KafkaDispatcher<Order>();
             var emailDispatcher = new KafkaDispatcher<String>()) {
            for (var i = 0; i < 10; i++) {

                var email = Math.random() + "@email.com";
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1).setScale(2, RoundingMode.HALF_UP);

                var order = new Order(orderId, amount, email);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                var emailCode = "Thank you for order! We are processing your order!";
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
            }
        }
    }
}
