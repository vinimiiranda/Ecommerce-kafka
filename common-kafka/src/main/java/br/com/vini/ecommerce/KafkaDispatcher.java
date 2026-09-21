package br.com.vini.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements AutoCloseable {

    /** Mesmo padrao do kafka-clients (delivery.timeout.ms). */
    private static final Duration DEFAULT_DELIVERY_TIMEOUT = Duration.ofMinutes(2);

    private final KafkaProducer<String, T> producer;

    public KafkaDispatcher() {
        this(DEFAULT_DELIVERY_TIMEOUT);
    }

    /**
     * @param deliveryTimeout tempo maximo para uma mensagem ser confirmada; depois disso send() falha.
     *                        Use um valor baixo em quem atende requisicoes (ex.: HTTP), para nao deixar o
     *                        cliente esperando minutos quando o cluster esta indisponivel.
     */
    public KafkaDispatcher(Duration deliveryTimeout) {
        this.producer = new KafkaProducer<>(properties(deliveryTimeout));
    }

    private static Properties properties(Duration deliveryTimeout) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCluster.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        // Durabilidade: a mensagem so e confirmada depois de replicada (min.insync.replicas do topico),
        // e a idempotencia evita duplicatas quando o produtor reenvia apos uma troca de lider.
        // Sao os padroes do kafka-clients, mas ficam explicitos para nao dependerem de versao.
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // delivery.timeout.ms precisa ser >= request.timeout.ms (+ linger); os demais sao derivados dele
        var deliveryMs = deliveryTimeout.toMillis();
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(deliveryMs));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(Math.min(30_000, deliveryMs / 2)));
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Math.min(60_000, deliveryMs)));
        return properties;
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
