package br.com.vini.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService implements AutoCloseable {

    private static final String DEFAULT_URL = "jdbc:sqlite:users_database.db";

    private final Connection connection;

    CreateUserService() throws SQLException {
        this(DEFAULT_URL);
    }

    CreateUserService(String url) throws SQLException {
        connection = DriverManager.getConnection(url);
        try (var statement = connection.createStatement()) {
            statement.execute("create table if not exists Users(" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        }
    }

    public static void main(String[] args) throws Exception {
        try (var createUserService = new CreateUserService();
             var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                     "ECOMMERCE_NEW_ORDER",
                     createUserService::parse,
                     Order.class,
                     Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, registering user if new");
        System.out.println(record.value());
        var email = record.value().getEmail();
        if (createIfNew(email)) {
            System.out.println("Usuario " + email + " adicionado com sucesso");
        }
    }

    /**
     * Insere o usuario apenas se o e-mail ainda nao existe, em uma unica instrucao. Assim uma
     * mensagem reentregue (o Kafka garante "pelo menos uma vez") nao gera usuario duplicado.
     *
     * @return true se o usuario foi criado, false se o e-mail ja existia
     */
    boolean createIfNew(String email) throws SQLException {
        try (var insert = connection.prepareStatement(
                "insert into Users (uuid, email) " +
                        "select ?, ? where not exists (select 1 from Users where email = ?)")) {
            insert.setString(1, UUID.randomUUID().toString());
            insert.setString(2, email);
            insert.setString(3, email);
            return insert.executeUpdate() > 0;
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
