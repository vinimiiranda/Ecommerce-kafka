package br.com.vini.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements AutoCloseable {

    /** Pedidos com valor maior ou igual a este sao considerados fraude. */
    private static final BigDecimal FRAUD_THRESHOLD = new BigDecimal("4500");

    public static void main(String[] args) {
        try (var fraudService = new FraudDetectorService();
             var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                     "ECOMMERCE_NEW_ORDER",
                     fraudService::parse,
                     Order.class,
                     Map.of())) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        // simula o tempo de uma analise de fraude
        Thread.sleep(5000);
        var order = record.value();
        if (isFraud(order)) {
            System.out.println("Order is a fraud!!!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }

        System.out.println("Order processed");
    }

    static boolean isFraud(Order order) {
        return order.getAmount().compareTo(FRAUD_THRESHOLD) >= 0;
    }

    @Override
    public void close() {
        orderDispatcher.close();
    }
}
