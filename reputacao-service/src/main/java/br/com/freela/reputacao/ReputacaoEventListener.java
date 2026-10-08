package br.com.freela.reputacao;

import br.com.freela.reputacao.shared.EventMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class ReputacaoEventListener {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoEventListener.class);
    private final ReputacaoService reputacaoService;
    private final ProcessamentoMensagemRepository idempotenciaRepository;

    public ReputacaoEventListener(ReputacaoService reputacaoService, ProcessamentoMensagemRepository idempotenciaRepository) {
        this.reputacaoService = reputacaoService;
        this.idempotenciaRepository = idempotenciaRepository;
    }

    @KafkaListener(topics = "contrato.lifecycle.events", groupId = "reputacao-group")
    @Transactional
    public void onEvent(EventMessage message) {
        MDC.put("correlationId", message.correlationId() != null ? message.correlationId() : "");

        try {
            log.info("reputacao.evento.recebido eventId={} eventType={} contratoId={}",
                    message.eventId(), message.eventType(), message.aggregateId());

            if (idempotenciaRepository.existsById(message.eventId())) {
                log.warn("reputacao.evento.duplicado.ignorado eventId={}", message.eventId());
                return;
            }

            if ("ContratoConcluido".equals(message.eventType())) {
                JsonNode payload = message.payload();
                UUID freelancerId = UUID.fromString(payload.get("freelancerId").asText());
                BigDecimal valor = new BigDecimal(payload.get("valor").asText());

                reputacaoService.registrarContratoConcluido(message.aggregateId(), freelancerId, valor);
            }

            idempotenciaRepository.save(new ProcessamentoMensagem(message.eventId()));
            log.info("reputacao.processamento.sucesso eventId={} contratoId={}", message.eventId(), message.aggregateId());

        } catch (Exception e) {
            log.error("reputacao.processamento.falha eventId={}", message.eventId(), e);
            throw e;
        } finally {
            MDC.clear();
        }
    }
}