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
  4. Replicação com QUÓRUM (Entrega 2), em duas fases:
       4a. Pede a todos os nós que RESERVEM o assento para esta venda.
           Cada nó aceita no máximo uma reserva por assento.
       4b. Se a MAIORIA aceitou (2 de 3): CONFIRMA a venda em todos.
           Se não: CANCELA as reservas e recusa a compra.
  5. Sai da seção crítica.

Por que o quórum? A exclusão mútua trata um nó sem resposta como
desligado. Se a rede se divide, os dois lados poderiam vender o mesmo
assento. Com o quórum, só o lado com a maioria consegue vender: duas
maiorias sempre têm um nó em comum, e esse nó só aceita uma reserva.

E se a confirmação não chegou a um nó (falha rápida de rede)? Cada nó que
dá a permissão no passo 2 conta junto se já sabe que o assento foi vendido.
Assim, o nó que perdeu a confirmação corrige a sua cópia antes do passo 3.

Se duas vendas do mesmo assento se encontram ao copiar o estado de outro
nó, vale a venda que veio primeiro na ordem de Lamport: menor
(relogio_venda, no_origem). Todas as cópias escolhem a mesma.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from eleicao_bully import EleicaoBully
from exclusao_mutua import ExclusaoMutua
from persistencia import ArquivoCorrompido, Persistencia
from rede import NoIndisponivel, TempoEsgotado
from relogio_lamport import RelogioLamport

FILEIRAS = "ABCDE"
COLUNAS = 10
ASSENTOS = [f"{f}{c}" for f in FILEIRAS for c in range(1, COLUNAS + 1)]


class NoIngressos:
    def __init__(self, meu_id, todos_ids, rede, intervalo_heartbeat=2.0,
                 limite_inativo=6.0, espera_coordenador=3.0,
                 atraso_secao_critica=0.0, vendas_abertas=False, verbose=True,
                 arquivo_dados=None, reserva_expira=10.0):
        self.meu_id = meu_id
        self.todos_ids = sorted(todos_ids)
        self.ids_outros = [i for i in self.todos_ids if i != meu_id]
        # Quórum = maioria dos nós configurados, contando este (3 nós -> 2).
        self.maioria = len(self.todos_ids) // 2 + 1
        self.reserva_expira = reserva_expira
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
        self.vendidos = {}  # assento -> (comprador, no_origem, relogio) CONFIRMADAS
        # Reservas da fase 1 do quórum: assento -> (venda, instante em que expira).
        # Se quem reservou cair antes da fase 2, a reserva expira sozinha.
        self.reservas = {}
        self.vendas_abertas = vendas_abertas
        self.persistencia = Persistencia(arquivo_dados)
        self._pool = ThreadPoolExecutor(max_workers=max(4, len(self.ids_outros) * 4))

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
        self.log(f"nó iniciado; outros nós: {self.ids_outros}; quórum = "
                 f"{self.maioria} de {len(self.todos_ids)}")
        self._carregar_do_disco()
        self._sincronizar_estado()
        self._thread_monitor = threading.Thread(target=self._monitorar, daemon=True)
        self._thread_monitor.start()

    def parar(self):
        self._rodando = False

    def _carregar_do_disco(self):
        """Lê as vendas confirmadas e as reservas gravadas antes de o nó cair."""
        try:
            vendidos, abertas, reservas = self.persistencia.carregar()
        except ArquivoCorrompido as erro:
            # Não impede o nó de subir: ele recupera o estado dos outros nós.
            self.log(f"arquivo de dados inválido, ignorado: {erro}")
            return
        if not vendidos and not abertas and not reservas:
            return
        with self._lock_dados:
            self.vendidos.update(vendidos)
            self.vendas_abertas = abertas
            self.reservas.update(reservas)
        # O relógio volta a partir da maior venda gravada.
        maior = [v[2] for v in vendidos.values()] + [r[0][2] for r in reservas.values()]
        self.relogio.atualizar(max(maior, default=0))
        self.log(f"estado lido do disco: {len(vendidos)} assentos vendidos, "
                 f"{len(reservas)} reserva(s)")

    def _sincronizar_estado(self):
        """Ao iniciar, junta o mapa de assentos de TODOS os nós ativos.
        Assim, o nó recebe as vendas confirmadas enquanto estava fora."""
        copiou_de = []
        for destino in self.ids_outros:
            try:
                vendidos, abertas, rel = self.rede.obter_estado(destino)
            except (NoIndisponivel, TempoEsgotado):
                continue
            self.relogio.atualizar(rel)
            with self._lock_dados:
                for assento, venda in vendidos.items():
                    self._mesclar_venda(assento, venda, "réplica")
                if not copiou_de:
                    self.vendas_abertas = abertas
            copiou_de.append(destino)
        if copiou_de:
            with self._lock_dados:
                total = len(self.vendidos)
                self._gravar_disco()
            self.log(f"estado sincronizado com os nós {copiou_de}: {total} assentos vendidos")
        else:
            self.log("nenhum outro nó ativo; segue com o estado local")

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
        """Heartbeat para o líder, detecção de falha e sincronização periódica."""
        falhas_seguidas = 0
        ciclo = 0
        while self._rodando:
            ciclo += 1
            sincronizar = ciclo % self.CICLOS_ENTRE_SINCRONIAS == 0
            lider = self.eleicao.lider
            if lider is None:
                falhas_seguidas = 0
                self.eleicao.iniciar_eleicao()
            elif lider == self.meu_id:
                self._atualizar_ativos()
                self._conferir_nos_maiores()
                if sincronizar:
                    # O líder junta as vendas de todos os nós ativos...
                    for destino in [i for i in self.ativos if i != self.meu_id]:
                        self._sincronizar_com(destino)
            else:
                try:
                    lider_dele, ativos, rel = self.rede.heartbeat(
                        lider, self.meu_id, self.relogio.tick())
                    self.relogio.atualizar(rel)
                    falhas_seguidas = 0
                    if lider_dele == lider and ativos:
                        self.ativos = ativos
                    if sincronizar:
                        # ...e cada nó copia do líder o que estiver faltando.
                        self._sincronizar_com(lider)
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
    # Sincronização periódica (anti-entropia): a cada 3 heartbeats (~6 s).
    CICLOS_ENTRE_SINCRONIAS = 3

    def _sincronizar_com(self, destino):
        """Anti-entropia: copia de 'destino' as vendas confirmadas que este nó
        não tem. Corrige confirmações perdidas sem esperar alguém pedir o assento."""
        try:
            vendidos, _, rel = self.rede.obter_estado(destino)
        except (NoIndisponivel, TempoEsgotado):
            return
        self.relogio.atualizar(rel)
        with self._lock_dados:
            faltando = {a: v for a, v in vendidos.items() if self.vendidos.get(a) != v}
            for assento, venda in faltando.items():
                self._mesclar_venda(assento, venda, "réplica")
            if faltando:
                self._gravar_disco()
        if faltando:
            self.log(f"sincronização com o nó {destino}: {len(faltando)} venda(s) "
                     f"corrigida(s): {sorted(faltando)}")

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
            venda = (comprador, self.meu_id, self.relogio.tick())
            return self._vender_com_quorum(assento, venda)
        finally:
            self.mutex.sair(assento)

    # ------------------------------------------------------------------
    # Replicação com quórum (Entrega 2)
    # ------------------------------------------------------------------
    def _vender_com_quorum(self, assento, venda):
        """Fase 1: reserva em todos. Fase 2: confirma (maioria) ou cancela."""
        comprador, _, ts = venda

        # Fase 1, no próprio nó: a mesma regra que os outros nós aplicam.
        aceita, motivo, atual = self._reservar(assento, venda)
        if not aceita:
            return self._recusar(assento, motivo, atual)
        aceitaram, recusas, sem_resposta = [self.meu_id], [], []

        # Fase 1, nos outros nós, em paralelo.
        def reservar_em(destino):
            try:
                ok, mot, atu, rel = self.rede.reservar_assento(
                    destino, assento, venda, self.relogio.tick())
                self.relogio.atualizar(rel)
                return destino, ok, mot, atu
            except (NoIndisponivel, TempoEsgotado):
                return destino, None, None, None

        inicio = time.time()
        for destino, ok, mot, atu in self._pool.map(reservar_em, self.ids_outros):
            if ok:
                aceitaram.append(destino)
            elif ok is None:
                sem_resposta.append(destino)
            else:
                recusas.append((destino, mot, atu))
        lento = time.time() - inicio > self.reserva_expira / 2
        if lento:
            # Os votos demoraram demais: as reservas podem estar perto de
            # expirar nos outros nós. Por segurança, não confirma.
            self.log(f"reservas de {assento} demoraram demais; a venda não será confirmada")

        # Fase 2a: maioria -> confirma em todos.
        if len(aceitaram) >= self.maioria and not lento:
            self.log(f"quórum atingido para {assento}: nós {sorted(aceitaram)} aceitaram "
                     f"({len(aceitaram)} de {len(self.todos_ids)}, mínimo {self.maioria})")
            with self._lock_dados:
                self.reservas.pop(assento, None)
                self.vendidos[assento] = venda
                self._gravar_disco()
            self.log(f"VENDEU {assento} para {comprador}")
            self._em_todos(lambda d: self.rede.confirmar_venda(
                d, assento, venda, self.relogio.tick()), "confirmação")
            return True, f"Assento {assento} vendido para {comprador}", ts

        # Fase 2b: sem maioria -> desfaz as reservas que foram aceitas.
        self.log(f"SEM QUÓRUM para {assento}: só {sorted(aceitaram)} aceitaram "
                 f"(mínimo {self.maioria}); sem resposta: {sem_resposta}")
        self._cancelar_local(assento, venda)
        # Também tenta quem não respondeu: a reserva pode ter chegado e a
        # resposta se perdido. Se o nó estiver fora, a reserva expira sozinha.
        a_cancelar = [d for d in aceitaram if d != self.meu_id] + sem_resposta
        self._em_todos(lambda d: self.rede.cancelar_reserva(
            d, assento, venda, self.relogio.tick()), "cancelamento", a_cancelar)
        for _, mot, atu in recusas:
            if mot == "vendido":
                with self._lock_dados:
                    self._mesclar_venda(assento, atu, "recusa de reserva")
                return self._recusar(assento, mot, atu)
        if recusas:
            return self._recusar(assento, "reservado", recusas[0][2])
        return (False, f"Sem quórum: só {len(aceitaram)} de {len(self.todos_ids)} nós "
                f"aceitaram (mínimo {self.maioria}). Venda não realizada.", self.relogio.valor)

    def _em_todos(self, enviar, nome, destinos=None):
        """Envia uma mensagem da fase 2 a vários nós em paralelo."""
        def um(destino):
            try:
                self.relogio.atualizar(enviar(destino))
            except (NoIndisponivel, TempoEsgotado):
                self.log(f"{nome} para o nó {destino} falhou (nó fora do ar)")
        list(self._pool.map(um, self.ids_outros if destinos is None else destinos))

    def _recusar(self, assento, motivo, atual):
        if motivo == "vendido":
            self.log(f"compra RECUSADA: {assento} já é de {atual[0]}")
            return False, f"Assento {assento} já foi vendido para {atual[0]}", self.relogio.valor
        self.log(f"compra RECUSADA: {assento} está reservado para {atual[0]} (nó {atual[1]})")
        return (False, f"Assento {assento} está em disputa com outra compra (tente de novo)",
                self.relogio.valor)

    def _reservar(self, assento, venda, tentar_resolver=True):
        """Regra de cada nó na fase 1: aceita no máximo UMA reserva por assento.
        Devolve (aceita, motivo, venda_atual)."""
        with self._lock_dados:
            agora = time.time()
            if assento in self.vendidos:
                atual = self.vendidos[assento]
                return (True, "", None) if atual == venda else (False, "vendido", atual)
            reserva = self.reservas.get(assento)
            outra = reserva and reserva[0] != venda
            # Uma tentativa nova do MESMO vendedor substitui a antiga: ele só
            # faz uma tentativa por assento de cada vez (seção crítica).
            mesmo_vendedor = outra and reserva[0][1] == venda[1]
            if outra and not mesmo_vendedor:
                if reserva[1] > agora:
                    return False, "reservado", reserva[0]
                expirada = reserva[0]
            else:
                self.reservas[assento] = (venda, agora + self.reserva_expira)
                self._gravar_disco()
                return True, "", None
        # A reserva de outra venda expirou. Isso NÃO libera o assento sozinho:
        # o vendedor pode ter confirmado e a confirmação se perdido.
        if tentar_resolver and self._resolver_reserva_expirada(assento, expirada):
            return self._reservar(assento, venda, tentar_resolver=False)
        return False, "reservado", expirada

    def _resolver_reserva_expirada(self, assento, antiga):
        """Pergunta ao vendedor da reserva antiga se ele confirmou a venda.
        Confirmou: adota a venda. Não confirmou: libera o assento.
        Vendedor fora do ar: o assento continua bloqueado (em dúvida),
        porque é melhor travar do que vender duas vezes.
        Devolve True se a dúvida foi resolvida."""
        vendedor = antiga[1]
        if vendedor == self.meu_id:
            vendidos_dele = None          # tentativa minha que terminou sem limpar
        else:
            try:
                vendidos_dele, _, rel = self.rede.obter_estado(vendedor)
                self.relogio.atualizar(rel)
            except (NoIndisponivel, TempoEsgotado):
                self.log(f"reserva de {assento} em dúvida: o nó {vendedor} não responde; "
                         f"o assento fica bloqueado")
                return False
        with self._lock_dados:
            if self.reservas.get(assento, (None,))[0] != antiga:
                return True               # mudou enquanto eu perguntava
            del self.reservas[assento]
            venda = (vendidos_dele or {}).get(assento)
            if venda:
                self._mesclar_venda(assento, venda, "consulta ao vendedor")
            self._gravar_disco()
        self.log(f"reserva expirada de {assento} resolvida com o nó {vendedor}: "
                 f"{'vendido para ' + venda[0] if venda else 'assento livre'}")
        return True

    def _cancelar_local(self, assento, venda):
        with self._lock_dados:
            if self.reservas.get(assento, (None,))[0] == venda:
                del self.reservas[assento]
                self._gravar_disco()

    def _gravar_disco(self):
        """Chamar com self._lock_dados já adquirido."""
        try:
            self.persistencia.salvar(self.vendidos, self.vendas_abertas, self.reservas)
        except OSError as erro:
            self.log(f"erro ao gravar no disco: {erro!r}")

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
            self._gravar_disco()
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
            "quorum": self.maioria,
            "total_nos": len(self.todos_ids),
        }

    # ------------------------------------------------------------------
    # Mensagens de outros nós: quórum
    # ------------------------------------------------------------------
    def ao_receber_reserva(self, assento, venda, relogio):
        """Fase 1 pedida por outro nó. Devolve (aceita, motivo, atual, relogio)."""
        self.relogio.atualizar(relogio)
        aceita, motivo, atual = self._reservar(assento, venda)
        if aceita:
            self.log(f"reserva ACEITA: {assento} para {venda[0]} (nó {venda[1]})")
        else:
            self.log(f"reserva RECUSADA: {assento} para {venda[0]} (nó {venda[1]}); "
                     f"já {motivo} para {atual[0]}")
        return aceita, motivo, atual, self.relogio.tick()

    def ao_receber_confirmacao(self, assento, venda, relogio):
        """Fase 2a: a maioria aceitou esta venda, então ela vale.
        Substitui qualquer reserva diferente deste nó."""
        self.relogio.atualizar(relogio)
        with self._lock_dados:
            self.reservas.pop(assento, None)
            anterior = self.vendidos.get(assento)
            self.vendidos[assento] = venda
            self._gravar_disco()
        if anterior and anterior != venda:
            self.log(f"confirmação substitui {anterior[0]} por {venda[0]} em {assento}")
        self.log(f"confirmação recebida: {assento} vendido pelo nó {venda[1]} para {venda[0]}")
        return self.relogio.tick()

    def ao_receber_cancelamento(self, assento, venda, relogio):
        """Fase 2b: a venda não teve maioria; desfaz a reserva dela, se houver."""
        self.relogio.atualizar(relogio)
        self._cancelar_local(assento, venda)
        self.log(f"reserva cancelada: {assento} para {venda[0]} (nó {venda[1]})")
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
        self.reservas.pop(assento, None)   # assento vendido: a reserva não vale mais
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
            self._gravar_disco()
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
