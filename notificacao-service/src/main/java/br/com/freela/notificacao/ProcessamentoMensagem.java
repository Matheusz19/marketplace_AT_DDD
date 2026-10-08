package br.com.freela.notificacao;

import jakarta.persistence.*;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "mensagens_processadas")
public class ProcessamentoMensagem implements Persistable<UUID> {
    @Id
    private UUID eventId;
    private Instant processadoEm;

    @Transient
    private boolean isNew = true;

    protected ProcessamentoMensagem() {}

    public ProcessamentoMensagem(UUID eventId) {
        this.eventId = eventId;
        this.processadoEm = Instant.now();
    }

    @Override
    public UUID getId() { return eventId; }

    @Override
    public boolean isNew() { return isNew; }

    @PrePersist
    @PostLoad
    void markNotNew() { this.isNew = false; }
}
