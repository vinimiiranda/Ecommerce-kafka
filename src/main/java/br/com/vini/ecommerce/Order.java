package br.com.vini.ecommerce;

import java.math.BigDecimal;

public class Order {


    private final String userId, orderId;
    private final BigDecimal amount;

    public Order(String userId, String orderId, BigDecimal value) {
        this.userId = userId;
        this.orderId = orderId;
        this.amount = value;
    }
}
