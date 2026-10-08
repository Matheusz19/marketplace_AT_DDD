package br.com.freela.auditoria;

import br.com.freela.auditoria.shared.EventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuditoriaEventListener {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaEventListener.class);
    private final AuditoriaService auditoriaService;

    public AuditoriaEventListener(AuditoriaService auditoriaService) {
        this.auditoriaService = auditoriaService;
    }

    @KafkaListener(topics = "contrato.lifecycle.events", groupId = "auditoria-group")
    public void onEvent(EventMessage message) {
        MDC.put("correlationId", message.correlationId() != null ? message.correlationId() : "");

        try {
            log.info("auditoria.evento.recebido eventId={} eventType={} aggregateId={}",
                    message.eventId(), message.eventType(), message.aggregateId());

            auditoriaService.registrar(
                    message.eventId(),
                    message.aggregateId(),
                    message.eventType(),
                    message.correlationId(),
                    message.payload().toString()
            );

        } catch (Exception e) {
            log.error("auditoria.processamento.falha eventId={}", message.eventId(), e);
            throw e;
        } finally {
            MDC.clear();
        }
    }
}