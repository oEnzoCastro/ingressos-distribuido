"""
NÓ DO SISTEMA DE INGRESSOS (banco de dados distribuído)
=======================================================

Cada nó guarda uma CÓPIA COMPLETA do mapa de assentos (replicação).
Este arquivo junta as peças:

  - RelogioLamport  -> ordem dos eventos
  - ExclusaoMutua   -> só um nó vende um assento por vez
  - EleicaoBully    -> escolhe o líder (abre/fecha vendas, lista de ativos)

Fluxo de uma compra no nó N:
  1. Confere se as vendas estão abertas e se o assento existe.
  2. Pede acesso exclusivo ao assento (Ricart-Agrawala).
  3. Dentro da seção crítica: confere se o assento está livre.
  4. Se está livre: grava a venda e REPLICA para todos os outros nós.
  5. Sai da seção crítica.

Como a réplica acontece ANTES de sair da seção crítica, o próximo nó que
entrar já vê o assento como vendido.

E se a réplica não chegou (falha rápida de rede)? Cada nó que dá a
permissão no passo 2 conta junto se já sabe que o assento foi vendido.
Assim, o nó que perdeu a réplica corrige a sua cópia antes do passo 3.

Se duas vendas do mesmo assento se encontram (só numa divisão longa da
rede), vale a venda que veio primeiro na ordem de Lamport: menor
(relogio_venda, no_origem). Todas as cópias escolhem a mesma.
"""

import threading
import time

from eleicao_bully import EleicaoBully
from exclusao_mutua import ExclusaoMutua
from rede import NoIndisponivel, TempoEsgotado
from relogio_lamport import RelogioLamport

FILEIRAS = "ABCDE"
COLUNAS = 10
ASSENTOS = [f"{f}{c}" for f in FILEIRAS for c in range(1, COLUNAS + 1)]


class NoIngressos:
    def __init__(self, meu_id, todos_ids, rede, intervalo_heartbeat=2.0,
                 limite_inativo=6.0, espera_coordenador=3.0,
                 atraso_secao_critica=0.0, vendas_abertas=False, verbose=True):
        self.meu_id = meu_id
        self.todos_ids = sorted(todos_ids)
        self.ids_outros = [i for i in self.todos_ids if i != meu_id]
        self.rede = rede
        self.intervalo_heartbeat = intervalo_heartbeat
        self.limite_inativo = limite_inativo
        self.atraso_secao_critica = atraso_secao_critica
        self.verbose = verbose

        self.relogio = RelogioLamport()
        self.mutex = ExclusaoMutua(meu_id, self.ids_outros, rede, self.relogio, self.log,
                                   consultar_recurso=self._consultar_assento)
        self.eleicao = EleicaoBully(meu_id, self.todos_ids, rede, self.relogio, self.log,
                                    ao_mudar_lider=self._ao_mudar_lider,
                                    espera_coordenador=espera_coordenador)

        # Dados replicados
        self._lock_dados = threading.Lock()
        self.vendidos = {}  # assento -> (comprador, no_origem, relogio)
        self.vendas_abertas = vendas_abertas

        # Lista de nós ativos (mantida pelo líder)
        self._ultimo_contato = {}
        self.ativos = [meu_id]

        self._eventos = []
        self._lock_eventos = threading.Lock()
        self._rodando = False
        self._thread_monitor = None

    # ------------------------------------------------------------------
    # Log com relógio de Lamport
    # ------------------------------------------------------------------
    def log(self, texto):
        # Cada evento registrado é um evento de Lamport: soma 1 no relógio.
        # Assim, dois eventos do mesmo nó nunca têm o mesmo valor.
        rel = self.relogio.tick()
        with self._lock_eventos:
            self._eventos.append((rel, self.meu_id, texto))
        if self.verbose:
            print(f"[Nó {self.meu_id} | L={rel:>4}] {texto}", flush=True)

    def eventos(self):
        with self._lock_eventos:
            return list(self._eventos)

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------
    def iniciar(self):
        self._rodando = True
        self.log(f"nó iniciado; outros nós: {self.ids_outros}")
        self._sincronizar_estado()
        self._thread_monitor = threading.Thread(target=self._monitorar, daemon=True)
        self._thread_monitor.start()

    def parar(self):
        self._rodando = False

    def _sincronizar_estado(self):
        """Ao iniciar, copia o mapa de assentos de qualquer nó ativo."""
        for destino in self.ids_outros:
            try:
                vendidos, abertas, rel = self.rede.obter_estado(destino)
            except (NoIndisponivel, TempoEsgotado):
                continue
            self.relogio.atualizar(rel)
            with self._lock_dados:
                for assento, venda in vendidos.items():
                    self._mesclar_venda(assento, venda, "réplica")
                self.vendas_abertas = abertas
            self.log(f"estado copiado do nó {destino}: {len(vendidos)} assentos vendidos")
            return
        self.log("nenhum outro nó ativo; começa com o mapa vazio")

    def _monitorar(self):
        """Laço de fundo. Um erro inesperado não pode matar este laço:
        sem ele, o nó para de mandar heartbeat e de fazer eleições."""
        while self._rodando:
            try:
                self._ciclo_monitor()
            except Exception as erro:  # noqa: BLE001
                self.log(f"erro no monitor: {erro!r}; continua")
                time.sleep(self.intervalo_heartbeat)

    def _ciclo_monitor(self):
        """Heartbeat para o líder e detecção de falha."""
        falhas_seguidas = 0
        while self._rodando:
            lider = self.eleicao.lider
            if lider is None:
                falhas_seguidas = 0
                self.eleicao.iniciar_eleicao()
            elif lider == self.meu_id:
                self._atualizar_ativos()
                self._conferir_nos_maiores()
            else:
                try:
                    lider_dele, ativos, rel = self.rede.heartbeat(
                        lider, self.meu_id, self.relogio.tick())
                    self.relogio.atualizar(rel)
                    falhas_seguidas = 0
                    if lider_dele == lider and ativos:
                        self.ativos = ativos
                except (NoIndisponivel, TempoEsgotado):
                    # Uma falha isolada pode ser só a rede se reconectando.
                    # O líder só é considerado morto após FALHAS_PARA_SUSPEITAR seguidas.
                    falhas_seguidas += 1
                    if falhas_seguidas >= self.FALHAS_PARA_SUSPEITAR:
                        self.log(f"líder {lider} não responde ao heartbeat "
                                 f"({falhas_seguidas} vezes seguidas)")
                        self.eleicao.esquecer_lider()
                        falhas_seguidas = 0
                        continue
            time.sleep(self.intervalo_heartbeat)

    FALHAS_PARA_SUSPEITAR = 2

    def _conferir_nos_maiores(self):
        """Regra do Bully: o líder deve ser o MAIOR nó ativo.
        Se eu sou o líder e um nó maior está respondendo (ele voltou, ou
        houve dois líderes por uma falha de rede), faço uma nova eleição."""
        for maior in (i for i in self.ids_outros if i > self.meu_id):
            try:
                rel = self.rede.heartbeat(maior, self.meu_id, self.relogio.tick())[2]
                self.relogio.atualizar(rel)
            except (NoIndisponivel, TempoEsgotado):
                continue
            self.log(f"nó {maior} (id maior) está ativo; nova eleição")
            self.eleicao.esquecer_lider()
            return

    def _ao_mudar_lider(self, lider):
        if lider == self.meu_id:
            # Um nó só entra na lista de ativos quando manda o 1º heartbeat.
            self._ultimo_contato = {}
            self._atualizar_ativos()

    def _atualizar_ativos(self):
        agora = time.time()
        # list(...) faz uma cópia: outro thread pode incluir um nó enquanto
        # este percorre o dicionário, o que geraria um erro.
        contatos = list(self._ultimo_contato.items())
        vivos = [i for i, t in contatos if agora - t < self.limite_inativo]
        self.ativos = sorted(vivos + [self.meu_id])

    # ------------------------------------------------------------------
    # Operações do cliente
    # ------------------------------------------------------------------
    def comprar(self, assento: str, comprador: str):
        """Devolve (sucesso, mensagem, relogio)."""
        assento = assento.strip().upper()
        if assento not in ASSENTOS:
            return False, f"Assento {assento} não existe", self.relogio.valor
        if not self.vendas_abertas:
            return False, "As vendas estão fechadas", self.relogio.valor

        self.log(f"pedido de compra: {assento} para {comprador}")
        ok, infos = self.mutex.entrar(assento)
        if not ok:
            return False, "Não foi possível reservar o assento (tente de novo)", self.relogio.valor
        try:
            if self.atraso_secao_critica:
                time.sleep(self.atraso_secao_critica)  # só para a demonstração

            with self._lock_dados:
                # O que os outros nós contaram junto com a permissão.
                for venda in infos:
                    self._mesclar_venda(assento, venda, "permissão de outro nó")
                if assento in self.vendidos:
                    dono = self.vendidos[assento][0]
                    self.log(f"compra RECUSADA: {assento} já é de {dono}")
                    return False, f"Assento {assento} já foi vendido para {dono}", self.relogio.valor
                ts = self.relogio.tick()
                self.vendidos[assento] = (comprador, self.meu_id, ts)
            self.log(f"VENDEU {assento} para {comprador}")

            for destino in self.ids_outros:
                try:
                    rel = self.rede.replicar_venda(destino, assento, comprador,
                                                   self.meu_id, ts, self.relogio.tick())
                    self.relogio.atualizar(rel)
                except (NoIndisponivel, TempoEsgotado):
                    self.log(f"réplica para o nó {destino} falhou (nó fora do ar)")
            return True, f"Assento {assento} vendido para {comprador}", ts
        finally:
            self.mutex.sair(assento)

    def alterar_vendas(self, abertas: bool, relogio: int = 0):
        """Só o líder abre/fecha as vendas. Outros nós repassam ao líder.
        relogio > 0 quando o pedido veio repassado por outro nó."""
        if relogio:
            self.relogio.atualizar(relogio)
        lider = self.eleicao.lider
        acao = "abrir" if abertas else "fechar"
        if lider is None:
            return False, "Sem líder no momento (eleição em andamento)"
        if lider != self.meu_id:
            self.log(f"repassa o pedido de {acao} vendas ao líder {lider}")
            try:
                return self.rede.alterar_vendas(lider, abertas, self.relogio.tick())
            except (NoIndisponivel, TempoEsgotado):
                return False, f"O líder {lider} não respondeu"

        self.log(f"LÍDER decide {acao} as vendas")
        with self._lock_dados:
            self.vendas_abertas = abertas
        for destino in self.ids_outros:
            try:
                rel = self.rede.definir_vendas(destino, abertas, self.relogio.tick())
                self.relogio.atualizar(rel)
            except (NoIndisponivel, TempoEsgotado):
                pass
        return True, f"Vendas {'abertas' if abertas else 'fechadas'} pelo líder (nó {self.meu_id})"

    def estado(self):
        with self._lock_dados:
            return dict(self.vendidos), self.vendas_abertas, self.relogio.tick()

    def status(self):
        return {
            "id": self.meu_id,
            "lider": self.eleicao.lider or 0,
            "ativos": list(self.ativos),
            "relogio": self.relogio.valor,
            "vendas_abertas": self.vendas_abertas,
            "vendidos": len(self.vendidos),
            "total_assentos": len(ASSENTOS),
        }

    # ------------------------------------------------------------------
    # Mensagens de outros nós
    # ------------------------------------------------------------------
    def ao_receber_venda(self, assento, comprador, no_origem, relogio_venda, relogio):
        self.relogio.atualizar(relogio)
        self.log(f"réplica recebida: {assento} vendido pelo nó {no_origem} para {comprador}")
        with self._lock_dados:
            self._mesclar_venda(assento, (comprador, no_origem, relogio_venda), "réplica")
        return self.relogio.tick()

    # ------------------------------------------------------------------
    # Consistência das cópias
    # ------------------------------------------------------------------
    def _consultar_assento(self, assento):
        """Enviado junto com a permissão (Ricart-Agrawala)."""
        with self._lock_dados:
            return self.vendidos.get(assento)

    def _mesclar_venda(self, assento, venda, origem):
        """Junta uma venda vinda de outro nó com a cópia local.
        Chamar com self._lock_dados já adquirido.
        Conflito: vence a venda que veio primeiro na ordem de Lamport."""
        atual = self.vendidos.get(assento)
        if atual == venda:
            return
        if atual is None:
            self.vendidos[assento] = venda
            if origem != "réplica":
                self.log(f"cópia corrigida: {assento} já era de {venda[0]} "
                         f"(nó {venda[1]}); a réplica tinha sido perdida")
            return
        chave = lambda v: (v[2], v[1])   # (relogio_venda, no_origem)
        vencedora = min(atual, venda, key=chave)
        perdedora = venda if vencedora is atual else atual
        self.vendidos[assento] = vencedora
        self.log(f"CONFLITO em {assento}: fica {vencedora[0]} (L={vencedora[2]}), "
                 f"descarta {perdedora[0]} (L={perdedora[2]})")

    def ao_receber_definir_vendas(self, abertas, relogio):
        self.relogio.atualizar(relogio)
        with self._lock_dados:
            self.vendas_abertas = abertas
        self.log(f"líder avisou: vendas {'ABERTAS' if abertas else 'FECHADAS'}")
        return self.relogio.tick()

    def ao_receber_heartbeat(self, origem, relogio):
        self.relogio.atualizar(relogio)
        if self.eleicao.lider == self.meu_id:
            if origem not in self._ultimo_contato or \
               time.time() - self._ultimo_contato[origem] >= self.limite_inativo:
                self.log(f"nó {origem} está ativo")
            self._ultimo_contato[origem] = time.time()
            self._atualizar_ativos()
        return self.eleicao.lider or 0, list(self.ativos), self.relogio.tick()
