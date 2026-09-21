package br.com.vini.ecommerce;

/**
 * Endereco do cluster Kafka usado por producers e consumers.
 * Lista os dois brokers para que o cliente consiga conectar mesmo se um deles estiver fora do ar.
 * Pode ser sobrescrito pela variavel de ambiente KAFKA_BOOTSTRAP_SERVERS.
 */
final class KafkaCluster {

    static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092");

    private KafkaCluster() {
    }
}
