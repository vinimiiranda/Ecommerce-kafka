package br.com.vini.ecommerce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CreateUserServiceTest {

    @Test
    void createsTheUserOnlyOncePerEmail() throws Exception {
        try (var service = new CreateUserService("jdbc:sqlite::memory:")) {
            assertTrue(service.createIfNew("ana@email.com"), "primeira vez deve criar");
            assertFalse(service.createIfNew("ana@email.com"), "mensagem reentregue nao pode duplicar");
            assertTrue(service.createIfNew("bia@email.com"), "outro e-mail deve criar");
        }
    }

    @Test
    void reopeningTheDatabaseKeepsTheSchemaIdempotent() throws Exception {
        // "create table if not exists": subir o servico duas vezes nao pode falhar
        try (var first = new CreateUserService("jdbc:sqlite::memory:")) {
            assertTrue(first.createIfNew("ana@email.com"));
        }
        try (var second = new CreateUserService("jdbc:sqlite::memory:")) {
            assertTrue(second.createIfNew("ana@email.com"));
        }
    }
}
