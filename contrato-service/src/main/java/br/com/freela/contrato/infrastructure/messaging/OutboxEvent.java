package br.com.freela.contrato.infrastructure.messaging;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id private UUID id;
    @Column(nullable = false) private UUID aggregateId;
    @Column(nullable = false) private String eventType;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(nullable = false) private String correlationId;

    @Column(length = 64) private String traceId;
    @Column(length = 64) private String spanId;

    @Column(nullable = false) private Instant createdAt;
    @Column(nullable = false) private boolean processed;

    protected OutboxEvent() {}

    public OutboxEvent(UUID id, UUID aggregateId, String eventType, String payload, String correlationId, String traceId, String spanId) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.traceId = traceId;
        this.spanId = spanId;
        this.createdAt = Instant.now();
        this.processed = false;
    }

    public void markAsProcessed() { this.processed = true; }

    public UUID getId() { return id; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getCorrelationId() { return correlationId; }
    public String getTraceId() { return traceId; }
    public String getSpanId() { return spanId; }
    public Instant getCreatedAt() { return createdAt; }
}