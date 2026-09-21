# 🛒 Ecommerce Kafka — Arquitetura Orientada a Eventos

Sistema de e-commerce distribuído construído com **Java 21** e **Apache Kafka**, demonstrando comunicação assíncrona entre microsserviços através de eventos.

Em vez de serviços chamando uns aos outros diretamente, cada serviço **publica eventos** em tópicos do Kafka e outros serviços **reagem** a esses eventos de forma independente. Isso elimina acoplamento: o serviço de pedidos não sabe (nem precisa saber) quem consome seus eventos.

---

## 🧭 Fluxo de eventos

```
      servicehttpecommerce              service-new-order
      GET /new?email=&amount=           (linha de comando)
                |                               |
                +---------------+---------------+
                                | (Producers)
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
| **servicehttpecommerce** | API HTTP (Jetty, porta 8080). `GET /new?email=&amount=` valida a entrada (400 se inválida), publica o pedido e o e-mail e responde 200; se o Kafka não confirmar em 10s responde 503 | publica em `ECOMMERCE_NEW_ORDER` e `ECOMMERCE_SEND_EMAIL` |
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
| API HTTP | Jetty 11 + Servlet (Jakarta) |
| Testes | JUnit 5 |
| Build | Maven (projeto multi-módulo) |
| Log | SLF4J 2 Simple |

---

## ▶️ Como executar

**Pré-requisitos:** Java 21+, Maven e Docker (para o cluster Kafka de 3 brokers, em `localhost:19092`, `localhost:29092` e `localhost:39092`).

```bash
# 1. Clonar e compilar
git clone https://github.com/vinimiiranda/Ecommerce-kafka.git
cd Ecommerce-kafka
mvn clean install

# 2. Subir o cluster Kafka (docker-compose.yml na raiz do projeto)
docker compose up -d

# 3. Iniciar os consumidores (cada um em um terminal)
mvn -pl fraud-detector-service exec:java -Dexec.mainClass=br.com.vini.ecommerce.FraudDetectorService
mvn -pl service-users          exec:java -Dexec.mainClass=br.com.vini.ecommerce.CreateUserService
mvn -pl service-email          exec:java -Dexec.mainClass=br.com.vini.ecommerce.EmailService
mvn -pl log-service            exec:java -Dexec.mainClass=br.com.vini.ecommerce.LogService

# 4. Disparar os pedidos (escolha um)
mvn -pl service-new-order exec:java -Dexec.mainClass=br.com.vini.ecommerce.NewOrderMain   # 10 pedidos aleatórios
mvn -pl servicehttpecommerce exec:java -Dexec.mainClass=br.com.vini.ecommerce.HttpEcommerceService
curl "http://localhost:8080/new?email=ana@email.com&amount=250.75"                        # 1 pedido via HTTP
```

**Testes:** `mvn test` roda os testes unitários (serialização, regra de fraude, cadastro de usuário sem duplicar e validação da API HTTP). Não precisam de Kafka.

**Configuração** (variáveis de ambiente, todas opcionais):

| Variável | Padrão | Uso |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092,localhost:29092,localhost:39092` | endereço do cluster |
| `HTTP_PORT` | `8080` | porta da API HTTP |

Os consumers encerram de forma limpa ao receber Ctrl+C / SIGTERM: saem do grupo na hora, sem esperar o timeout de sessão, para os outros consumers assumirem as partições. O `CreateUserService` grava em `users_database.db`, criado no diretório onde o serviço é iniciado.

Os consumidores imprimem no console os eventos recebidos, com tópico, chave, partição e offset.

---

## 🛡️ Alta disponibilidade (3 brokers)

O `docker-compose.yml` sobe um cluster Kafka em modo KRaft com:

| Container | Papel | Porta no host |
|---|---|---|
| `ecommerce-kafka-1` | broker + controller | `19092` |
| `ecommerce-kafka-2` | broker + controller | `29092` |
| `ecommerce-kafka-4` | broker | `39092` |
| `ecommerce-kafka-3` | controller dedicado (desempata o quórum) | — |

- Todo tópico tem **3 réplicas** (uma em cada broker) e `min.insync.replicas=2`. Com `acks=all` (padrão do `kafka-clients`), uma mensagem só é confirmada depois de estar em pelo menos 2 brokers: a queda de **um** broker não perde nenhuma mensagem confirmada e as escritas continuam funcionando.
- Com **2 brokers fora do ar** (só 1 vivo) o cluster prioriza durabilidade sobre disponibilidade: as escritas são **recusadas** (o produtor recebe erro) em vez de aceitas sem réplica, e os serviços que consomem em grupo (todos os do projeto) **pausam**, pois o grupo precisa gravar seus offsets no `__consumer_offsets`, que também exige 2 réplicas. Nenhum dado é perdido e tudo retoma sozinho quando os brokers voltam.
- `unclean.leader.election.enable=false`: uma réplica desatualizada nunca vira líder; o Kafka prefere ficar indisponível a descartar mensagens já confirmadas.
- Os tópicos internos (`__consumer_offsets` e o log de transações) também usam fator de replicação 3, então os consumers não perdem a posição quando um broker cai.
- O quórum do KRaft precisa de maioria, por isso há 3 votantes (`kafka-1`, `kafka-2` e `kafka-3`).
- Os clientes (`KafkaDispatcher` e `KafkaService`) conectam usando os três brokers. Para usar outro endereço, defina a variável de ambiente `KAFKA_BOOTSTRAP_SERVERS`.
- Os tópicos do projeto são criados pelo container `ecommerce-kafka-init` com 3 partições e 3 réplicas.

Para testar o failover:

```bash
docker kill ecommerce-kafka-1     # derruba um broker
# ...envie pedidos: continuam sendo produzidos e consumidos...
docker start ecommerce-kafka-1    # ao voltar, ele ressincroniza sozinho
```

---

## 🗺️ Próximos passos

- [ ] `docker-compose.yml` subindo também os serviços (hoje só o cluster Kafka)
- [ ] Tratamento de falhas com dead letter topic
- [ ] Testes de integração com Testcontainers (os unitários já existem)
- [ ] Dashboard de monitoramento dos tópicos

---

## 👤 Autor

**Vinicius de Miranda Melo**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/vinicius-miranda-melo)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/vinimiiranda)
