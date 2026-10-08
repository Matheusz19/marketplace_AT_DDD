package br.com.freela.notificacao;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface ProcessamentoMensagemRepository extends JpaRepository<ProcessamentoMensagem, UUID> {}