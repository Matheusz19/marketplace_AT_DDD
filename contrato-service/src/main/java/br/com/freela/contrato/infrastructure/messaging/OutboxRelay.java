package br.com.freela.contrato.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxRelay(OutboxRepository repository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       ObjectMapper objectMapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publishEvents() {
        List<OutboxEvent> events = repository.findByProcessedFalseOrderByCreatedAtAsc();
        if (events.isEmpty()) {
            return;
        }

        for (OutboxEvent event : events) {
            String correlationId = event.getCorrelationId();

            try {
                if (correlationId != null) {
                    MDC.put("correlationId", correlationId);
                }

                ObjectNode envelope = objectMapper.createObjectNode();
                envelope.put("eventId", event.getId().toString());
                envelope.put("eventType", event.getEventType());
                envelope.put("aggregateId", event.getAggregateId().toString());
                envelope.put("occurredAt", event.getCreatedAt().toString());
                envelope.put("correlationId", correlationId);
                envelope.set("payload", objectMapper.readTree(event.getPayload()));

                String jsonMessage = objectMapper.writeValueAsString(envelope);

                kafkaTemplate.send("contrato.lifecycle.events", event.getAggregateId().toString(), jsonMessage)
                        .whenComplete((result, ex) -> {
                            if (correlationId != null) {
                                MDC.put("correlationId", correlationId);
                            }
                            try {
                                if (ex == null) {
                                    log.info("kafka.publish.sucesso eventId={} topic=contrato.lifecycle.events", event.getId());
                                } else {
                                    log.error("kafka.publish.falha eventId={}", event.getId(), ex);
                                }
                            } finally {
                                MDC.remove("correlationId");
                            }
                        });

                event.markAsProcessed();
                repository.save(event);

            } catch (Exception e) {
                log.error("Erro ao processar envio do evento outbox eventId={}", event.getId(), e);
            } finally {
                MDC.remove("correlationId");
            }
        }
    }
}