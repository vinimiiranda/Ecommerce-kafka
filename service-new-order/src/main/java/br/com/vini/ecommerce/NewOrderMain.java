package br.com.vini.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) throws Exception {
        try (var orderdispatcher = new KafkaDispatcher<Order>()) {
            try (var emaildispatcher = new KafkaDispatcher<String>()) {
                for (var i = 0; i < 10; i++) {

                    var email = Math.random() + "email.com";
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, email);
                    orderdispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                    var emailCode = "Thank you for order! We are processing your order!";
                    emaildispatcher.send("ECOMMERCE_NEW_EMAIL", email, emailCode);

                }

            }
        }
    }
}