# Especificação de Mensageria - Freela Marketplace

## 1. Visão Geral
A comunicação assíncrona do sistema utiliza o padrão Publish-Subscribe via Apache Kafka. O particionamento é feito utilizando o identificador do contrato (`contratoId`) como chave (Kafka Key), garantindo a ordenação (FIFO) dos eventos para um mesmo contrato.

## 2. Evento: ContratoCriado
* **Tópico Kafka:** `contrato.lifecycle.events`
* **Serviço Produtor:** `contrato-service`
* **Serviços Consumidores:** `notificacao-service`, `auditoria-service`, `reputacao-service` (ignora criação, mas ouve o tópico)
* **Chave de Particionamento (Key):** `contratoId` (UUID)
* **Estrutura do Payload (Envelope):**
    * `eventId` (UUID): Identificador único do evento.
    * `eventType` (String): Tipo do evento (ex: "ContratoCriado").
    * `aggregateId` (UUID): Identificador do domínio afetado (`contratoId`).
    * `occurredAt` (Timestamp): Data e hora da ocorrência.
    * `correlationId` (String): Identificador de rastreamento do API Gateway.
    * `payload` (JSON): Dados específicos do evento.

## 3. Evento: ContratoConcluido
- **Tópico Kafka:** `contrato.lifecycle.events`
- **Produtor:** `contrato-service`
- **Consumidores:** `notificacao-service`, `reputacao-service`, `auditoria-service`
- **Chave de Particionamento:** `contratoId` (UUID)
- **Estrutura do Payload (JSON):**
  - `contratoId` (UUID), `freelancerId` (UUID), `valor` (Decimal)

**Exemplo de Mensagem (JSON):**
```json
{
  "eventId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "eventType": "ContratoCriado",
  "aggregateId": "123e4567-e89b-12d3-a456-426614174000",
  "occurredAt": "2023-10-25T10:00:00Z",
  "correlationId": "8b5f3811-9a7c-473d-82d6-11f4c7d0d0e6",
  "payload": {
    "clienteId": "a1b2c3d4-e89b-12d3-a456-426614174000",
    "freelancerId": "f9e8d7c6-e89b-12d3-a456-426614174000",
    "titulo": "Desenvolvimento de API REST",
    "valor": 5000.00
  }
}