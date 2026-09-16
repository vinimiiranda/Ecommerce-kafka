# 🛒 Ecommerce Kafka — Arquitetura Orientada a Eventos

Sistema de e-commerce distribuído construído com **Java 21** e **Apache Kafka**, demonstrando comunicação assíncrona entre microsserviços através de eventos.

Em vez de serviços chamando uns aos outros diretamente, cada serviço **publica eventos** em tópicos do Kafka e outros serviços **reagem** a esses eventos de forma independente. Isso elimina acoplamento: o serviço de pedidos não sabe (nem precisa saber) quem consome seus eventos.

---

## 🧭 Fluxo de eventos

```
                          service-new-order  (Producer)
                                   |
                 +-----------------+------------------+
                 v                                    v
        ECOMMERCE_NEW_ORDER                 ECOMMERCE_SEND_EMAIL
                 |                                    |
        +--------+---------+                          v
        v                  v                   service-email
fraud-detector-service  service-users            (Consumer)
(Consumer + Producer)   (Consumer)
        |                  |
        |                  +--> grava usuario novo (SQLite)
        v
  +-----+------+
  v            v
ECOMMERCE_    ECOMMERCE_
ORDER_        ORDER_
APPROVED      REJECTED

        log-service  ->  consome o padrao ECOMMERCE.*  (todos os topicos)
```

---

## 🧩 Serviços

| Módulo | Papel | Tópicos |
|---|---|---|
| **common-kafka** | Biblioteca compartilhada: `KafkaDispatcher` (producer genérico), `KafkaService` (consumer genérico), serialização JSON com Gson e o modelo `Order` | — |
| **service-new-order** | Producer. Gera pedidos e dispara o evento de pedido e o de e-mail de confirmação | publica em `ECOMMERCE_NEW_ORDER` e `ECOMMERCE_SEND_EMAIL` |
| **fraud-detector-service** | Consumer + Producer. Avalia cada pedido e o classifica como aprovado ou fraudulento (regra: valor maior ou igual a 4500) | consome `ECOMMERCE_NEW_ORDER`, publica em `ECOMMERCE_ORDER_APPROVED` / `ECOMMERCE_ORDER_REJECTED` |
| **service-users** | Consumer. Cadastra o cliente em um banco SQLite caso ainda não exista | consome `ECOMMERCE_NEW_ORDER` |
| **service-email** | Consumer. Simula o envio do e-mail de confirmação | consome `ECOMMERCE_SEND_EMAIL` |
| **log-service** | Consumer com subscrição por regex (`ECOMMERCE.*`) — registra tudo que trafega no sistema | consome todos os tópicos `ECOMMERCE*` |

---

## 🧠 Conceitos aplicados

- **Producer e Consumer genéricos** — `KafkaDispatcher<T>` e `KafkaService<T>` encapsulam a API do Kafka e são reaproveitados por todos os serviços, evitando código repetido.
- **Serialização customizada** — `GsonSerializer` e `GsonDeserializer` permitem trafegar objetos Java como JSON nos tópicos.
- **Consumer groups** — cada serviço usa seu próprio grupo, então todos recebem o mesmo pedido de forma independente.
- **Chave de partição** — o e-mail do cliente é usado como chave, garantindo que pedidos do mesmo cliente caiam sempre na mesma partição e mantenham a ordem.
- **Subscrição por padrão** — o `log-service` mostra como um consumidor pode escutar vários tópicos com uma expressão regular.
- **Serviços desacoplados** — adicionar um novo consumidor não exige alterar nenhum serviço existente.

---

## 🧰 Stack

| Categoria | Tecnologias |
|---|---|
| Linguagem | Java 21 |
| Mensageria | Apache Kafka (`kafka-clients` 4.3.1) |
| Serialização | Gson |
| Persistência | SQLite (JDBC) |
| Build | Maven (projeto multi-módulo) |
| Log | SLF4J Simple |

---

## ▶️ Como executar

**Pré-requisitos:** Java 21+, Maven e um broker Kafka rodando em `localhost:9092`.

```bash
# 1. Clonar e compilar
git clone https://github.com/vinimiiranda/Ecommerce-kafka.git
cd Ecommerce-kafka
mvn clean install

# 2. Subir o broker Kafka (docker-compose.yml na raiz do projeto)
docker compose up -d

# 3. Iniciar os consumidores (cada um em um terminal)
mvn -pl fraud-detector-service exec:java -Dexec.mainClass=br.com.vini.ecommerce.FraudDetectorService
mvn -pl service-users          exec:java -Dexec.mainClass=br.com.vini.ecommerce.CreateUserService
mvn -pl service-email          exec:java -Dexec.mainClass=br.com.vini.ecommerce.EmailService
mvn -pl log-service            exec:java -Dexec.mainClass=br.com.vini.ecommerce.LogService

# 4. Disparar os pedidos
mvn -pl service-new-order exec:java -Dexec.mainClass=br.com.vini.ecommerce.NewOrderMain
```

Os consumidores imprimem no console os eventos recebidos, com tópico, chave, partição e offset.

---

## 🗺️ Próximos passos

- [ ] `docker-compose.yml` subindo Kafka e todos os serviços com um comando
- [ ] Tratamento de falhas com dead letter topic
- [ ] Testes automatizados com Testcontainers
- [ ] Dashboard de monitoramento dos tópicos

---

## 👤 Autor

**Vinicius de Miranda Melo**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/vinicius-miranda-melo)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vinimiiranda)
