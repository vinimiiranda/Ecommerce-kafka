package br.com.vini.ecommerce;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FraudDetectorServiceTest {

    private static Order orderOf(String amount) {
        return new Order("id", new BigDecimal(amount), "cliente@email.com");
    }

    @Test
    void amountBelowThresholdIsApproved() {
        assertFalse(FraudDetectorService.isFraud(orderOf("1")));
        assertFalse(FraudDetectorService.isFraud(orderOf("4499.99")));
    }

    @Test
    void amountAtOrAboveThresholdIsFraud() {
        assertTrue(FraudDetectorService.isFraud(orderOf("4500")));
        assertTrue(FraudDetectorService.isFraud(orderOf("4500.00")));
        assertTrue(FraudDetectorService.isFraud(orderOf("4500.01")));
        assertTrue(FraudDetectorService.isFraud(orderOf("99999")));
    }
}
