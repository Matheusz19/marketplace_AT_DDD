# 🚀 Plataforma Freela - Event-Driven Architecture

Plataforma de microsserviços para gestão de contratos de freelancers, construída com Spring Boot 3 e arquitetura orientada a eventos (Event-Driven Architecture) utilizando Apache Kafka.

## 🏗 Arquitetura da Solução

O sistema é composto por múltiplos serviços que se comunicam de forma assíncrona para garantir alta disponibilidade e baixo acoplamento:

- **Contrato-Service (Produtor):** Responsável por gerenciar o ciclo de vida dos contratos. Utiliza o **Outbox Pattern** (tabela `outbox_events`) para garantir que dados de negócio e eventos de domínio sejam salvos atomicamente.
- **Notificacao-Service (Consumidor):** Envia alertas para freelancers e clientes sobre status de contratos.
- **Reputacao-Service (Consumidor):** Calcula e atualiza o score financeiro/reputacional dos freelancers após a conclusão dos trabalhos.
- **Auditoria-Service (Consumidor):** Mantém um histórico imutável (append-only) de todas as ações da plataforma.

### 🛡 Padrões de Resiliência Aplicados
- **Outbox Pattern:** Prevenção de dupla escrita (Dual-Write Problem).
- **Consumo Particionado (Ordering):** O `contratoId` é usado como *Message Key* no Kafka, garantindo que eventos de um mesmo contrato sejam processados em ordem.
- **Idempotência:** Tabelas locais nos consumidores validam o `eventId` para ignorar silenciosamente mensagens duplicadas (at-least-once delivery).

## 🛠 Tecnologias e Infraestrutura

A infraestrutura é totalmente conteinerizada via Docker Compose, englobando:

* **Java 17+ & Spring Boot 3:** Framework base.
* **PostgreSQL:** Banco de dados relacional.
* **Apache Kafka:** Message broker para streaming de eventos.
* **Zipkin:** Rastreamento distribuído (Distributed Tracing).
* **Loki & Promtail:** Centralização de logs.
* **Grafana:** Dashboard de observabilidade.
* **Kafka-UI:** Interface visual de gestão do cluster Kafka.

## 🔍 Observabilidade Integrada

O sistema conta com logs ricos via `MDC` (Mapped Diagnostic Context) do SLF4j. Todo log do sistema emite um `correlationId` injetado automaticamente pelos headers do Kafka, permitindo rastrear o ciclo de vida completo de uma requisição pelo Grafana.

## 🚀 Como Executar o Projeto

### 1. Subir a Infraestrutura
Na raiz do projeto onde está o `docker-compose.yml`, execute:

```bash
docker compose up -d
```

Aguarde a inicialização. Isso subirá o Kafka, PostgreSQL, Zipkin, Loki e Grafana.

### 2. Painéis de Controle Disponíveis
- **Kafka-UI:** http://localhost:8090
- **Grafana (Logs):** http://localhost:3000 *(User: admin | Pass: admin)*

### 3. Rodar as Aplicações Spring Boot
No seu ambiente de desenvolvimento (IDE), inicie os microsserviços na seguinte ordem recomendada:
1. `contrato-service`
2. `notificacao-service`
3. `reputacao-service`
4. `auditoria-service`

### 4. Testar o Fluxo (Criação de Contrato)
Faça uma requisição HTTP POST para criar um contrato e desencadear os eventos:

**Endpoint:** `POST http://localhost:8080/api/contratos` *(ajuste a porta conforme seu serviço)*

```json
{
  "clienteId": "123e4567-e89b-12d3-a456-426614174000",
  "freelancerId": "987e6543-e21b-34d5-c678-426614174999",
  "titulo": "Desenvolvimento Backend",
  "valor": 5000.00
}
```

### 5. Validando as Evidências
* **Kafka-UI:** Verifique o tópico `contrato.lifecycle.events`.
* **Grafana:** Busque o `correlationId` retornado nos logs da IDE dentro da aba "Explore" (Data Source: Loki) para ver os logs unificados de todos os serviços.

---
