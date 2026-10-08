package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class AuditoriaService {
 private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
 private final EventoAuditoriaRepository repository;

 public AuditoriaService(EventoAuditoriaRepository repository) {
  this.repository = repository;
 }

 @Transactional
 public void registrar(UUID eventId, UUID aggregateId, String eventType, String correlationId, String payload) {
  log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",
          eventId, aggregateId, eventType, correlationId);

  // TRATAMENTO DE MENSAGENS DUPLICADAS (IDEMPOTÊNCIA - Ponto 5)
  if (repository.existsById(eventId)) {
   log.warn("auditoria.evento.duplicado.ignorado eventId={}", eventId);
   return;
  }

  var evento = new EventoAuditoria(eventId, aggregateId, eventType, correlationId, payload);
  repository.save(evento);

  log.info("auditoria.registro.sucesso eventId={} aggregateId={} eventType={}",
          evento.getId(), aggregateId, eventType);
 }
}
