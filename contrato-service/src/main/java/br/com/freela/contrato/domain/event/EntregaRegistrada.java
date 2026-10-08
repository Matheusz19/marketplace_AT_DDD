package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(UUID eventId, Instant occurredAt, UUID contratoId) implements DomainEvent {
    public static EntregaRegistrada novo(Contrato c) {
        return new EntregaRegistrada(UUID.randomUUID(), Instant.now(), c.id());
    }
    @Override public String eventType() { return "EntregaRegistrada"; }
}
