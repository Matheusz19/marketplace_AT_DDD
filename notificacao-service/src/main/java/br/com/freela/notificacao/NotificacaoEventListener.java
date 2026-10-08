package br.com.freela.notificacao;

import br.com.freela.notificacao.shared.EventMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
public class NotificacaoEventListener {
    private static final Logger log = LoggerFactory.getLogger(NotificacaoEventListener.class);
    private final NotificacaoService notificacaoService;
    private final ProcessamentoMensagemRepository idempotenciaRepository;

    public NotificacaoEventListener(NotificacaoService notificacaoService, ProcessamentoMensagemRepository idempotenciaRepository) {
        this.notificacaoService = notificacaoService;
        this.idempotenciaRepository = idempotenciaRepository;
    }

    @KafkaListener(topics = "contrato.lifecycle.events", groupId = "notificacao-group")
    @Transactional
    public void onEvent(EventMessage message) {
        MDC.put("correlationId", message.correlationId() != null ? message.correlationId() : "");

        try {
            log.info("notificacao.evento.recebido eventId={} eventType={} contratoId={}",
                    message.eventId(), message.eventType(), message.aggregateId());

            if (idempotenciaRepository.existsById(message.eventId())) {
                log.warn("notificacao.evento.duplicado.ignorado eventId={}", message.eventId());
                return;
            }

            JsonNode payload = message.payload();
            UUID contratoId = message.aggregateId();

            if ("ContratoCriado".equals(message.eventType())) {
                UUID clienteId = UUID.fromString(payload.get("clienteId").asText());
                UUID freelancerId = UUID.fromString(payload.get("freelancerId").asText());
                String titulo = payload.get("titulo").asText();

                notificacaoService.registrar(contratoId, clienteId, "CONTRATO_CRIADO",
                        "Seu contrato '" + titulo + "' foi criado com sucesso.");
                notificacaoService.registrar(contratoId, freelancerId, "NOVO_CONTRATO",
                        "Você recebeu uma nova proposta de contrato: '" + titulo + "'.");
            }

            idempotenciaRepository.save(new ProcessamentoMensagem(message.eventId()));
            log.info("notificacao.processamento.sucesso eventId={} contratoId={}", message.eventId(), contratoId);

        } catch (Exception e) {
            log.error("notificacao.processamento.falha eventId={} contratoId={}", message.eventId(), message.aggregateId(), e);
            throw e;
        } finally {
            MDC.clear();
        }
    }
}