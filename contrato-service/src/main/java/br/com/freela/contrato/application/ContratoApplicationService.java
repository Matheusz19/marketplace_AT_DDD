package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.config.CorrelationIdContext;
import br.com.freela.contrato.infrastructure.messaging.OutboxEvent;
import br.com.freela.contrato.infrastructure.messaging.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public ContratoApplicationService(ContratoRepository repository, OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        salvarEventosOutbox(contrato);

        Contrato salvo = repository.salvar(contrato);

        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        Contrato contrato = buscar(id);
        contrato.registrarEntrega();

        salvarEventosOutbox(contrato);

        Contrato salvo = repository.salvar(contrato);

        log.info("contrato.entrega.sucesso contratoId={}", salvo.id());
        return salvo;
    }

    @Transactional
    public Contrato concluirContrato(UUID id) {
        Contrato contrato = buscar(id);
        contrato.concluir();

        salvarEventosOutbox(contrato);

        Contrato salvo = repository.salvar(contrato);

        log.info("contrato.conclusao.sucesso contratoId={}", salvo.id());
        return salvo;
    }

    private void salvarEventosOutbox(Contrato contrato) {
        String correlationId = CorrelationIdContext.get();
        for (DomainEvent event : contrato.pullDomainEvents()) {
            try {
                String payloadJson = objectMapper.writeValueAsString(event);
                OutboxEvent outboxEvent = new OutboxEvent(
                        event.eventId(), contrato.id(), event.eventType(), payloadJson, correlationId
                );
                outboxRepository.save(outboxEvent);
                log.info("contrato.evento.outbox.salvo eventId={} eventType={}", event.eventId(), event.eventType());
            } catch (Exception e) {
                throw new RuntimeException("Erro ao serializar evento de dominio", e);
            }
        }
    }
}