package br.com.vini.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements AutoCloseable {

    /**
     * Registros devolvidos por poll(). O consumer so e considerado vivo se chamar poll() dentro de
     * max.poll.interval.ms (5 min); com o padrao de 500 registros, um handler lento (ex.: 5s por
     * pedido) estouraria esse prazo, o consumer seria expulso do grupo e o lote reprocessado.
     */
    private static final String MAX_POLL_RECORDS = "10";

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
    }

    /**
     * Consome ate o processo receber um sinal de encerramento (Ctrl+C / SIGTERM). Nesse caso sai do
     * grupo na hora, sem esperar o session.timeout, para os outros consumers assumirem as particoes.
     */
    public void run() {
        var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join(15_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "kafka-service-shutdown"));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() + " registros");
                    for (var record : records) {
                        process(record);
                    }
                }
            }
        } catch (WakeupException e) {
            System.out.println("Encerrando consumer...");
        } finally {
            consumer.close();
        }
    }

    private void process(ConsumerRecord<String, T> record) {
        try {
            parse.consume(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logFailure(record, e);
        } catch (ExecutionException | SQLException e) {
            logFailure(record, e);
        }
    }

    private void logFailure(ConsumerRecord<String, T> record, Exception e) {
        System.err.println("Falha ao processar " + record.topic() + "-" + record.partition() + "@" + record.offset()
                + " (key=" + record.key() + "): " + e);
        e.printStackTrace();
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
