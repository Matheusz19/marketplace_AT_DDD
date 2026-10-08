package br.com.freela.auditoria.shared;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record EventMessage(
        UUID eventId,
        String eventType,
        UUID aggregateId,
        Instant occurredAt,
        String correlationId,
        JsonNode payload
) {}