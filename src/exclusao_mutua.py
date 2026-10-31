"""
EXCLUSÃO MÚTUA DISTRIBUÍDA - ALGORITMO DE RICART-AGRAWALA
=========================================================

Problema: dois nós não podem vender o mesmo assento ao mesmo tempo.

Neste sistema, cada ASSENTO é um recurso separado. Assim, vendas de
assentos diferentes acontecem em paralelo, e só vendas do MESMO assento
esperam umas pelas outras.

Como funciona:
  1. Para usar o assento X, o nó cria um timestamp de Lamport (ts).
  2. O nó envia "PedirAcesso(X, ts)" para TODOS os outros nós.
  3. O nó entra na seção crítica só quando TODOS respondem.
  4. Quando um nó recebe um pedido para X:
       - se ele não quer X: responde na hora;
       - se ele está USANDO X: segura a resposta até terminar;
       - se ele também QUER X: compara (ts, id). O menor par tem prioridade.
         Se o pedido recebido tem prioridade, ele responde na hora.
         Se não, ele segura a resposta até terminar.
  5. Ao sair da seção crítica, o nó libera todas as respostas seguradas.

Detalhe do gRPC: "segurar a resposta" é simplesmente não retornar da função
PedirAcesso ainda. O thread que atende o pedido espera em uma condição
(threading.Condition) até poder responder.

Falhas (simples, para a Entrega 1):
  - Nó desligado (NoIndisponivel): não pode vender nada, então é ignorado.
  - Nó ativo que não responde a tempo (TempoEsgotado): a compra é cancelada,
    para não arriscar uma venda dupla.
"""

import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from rede import NoIndisponivel, TempoEsgotado

QUERENDO = "QUERENDO"
DENTRO = "DENTRO"


class ExclusaoMutua:
    def __init__(self, meu_id, ids_outros, rede, relogio, log):
        self.meu_id = meu_id
        self.ids_outros = list(ids_outros)
        self.rede = rede
        self.relogio = relogio
        self.log = log

        self._cond = threading.Condition()
        # recurso -> (estado, timestamp do meu pedido)
        self._estado = {}
        # Um lock local por recurso: dois clientes no MESMO nó que querem
        # o mesmo assento fazem fila aqui antes de iniciar o algoritmo.
        self._locks_locais = defaultdict(threading.Lock)
        self._lock_dict = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=max(4, len(self.ids_outros) * 4))

    def _lock_local(self, recurso):
        with self._lock_dict:
            return self._locks_locais[recurso]

    # ------------------------------------------------------------------
    # Lado de quem PEDE
    # ------------------------------------------------------------------
    def entrar(self, recurso: str) -> bool:
        """Bloqueia até ter acesso exclusivo ao recurso.
        Devolve False se algum nó ativo não respondeu a tempo."""
        self._lock_local(recurso).acquire()

        with self._cond:
            ts = self.relogio.tick()
            self._estado[recurso] = (QUERENDO, ts)
        self.log(f"pede acesso a {recurso} (ts={ts}) para os nós {self.ids_outros}")

        # Envia os pedidos em paralelo e espera todas as respostas.
        futuros = [self._pool.submit(self._pedir, destino, recurso, ts)
                   for destino in self.ids_outros]
        todos_ok = all(f.result() for f in futuros)

        if not todos_ok:
            self.log(f"acesso a {recurso} cancelado: um nó não respondeu a tempo")
            self.sair(recurso)
            return False

        with self._cond:
            self._estado[recurso] = (DENTRO, ts)
        self.log(f"ENTROU na seção crítica de {recurso}")
        return True

    def _pedir(self, destino, recurso, ts) -> bool:
        try:
            relogio_resposta = self.rede.pedir_acesso(destino, recurso, self.meu_id, ts)
            self.relogio.atualizar(relogio_resposta)
            return True
        except NoIndisponivel:
            # Nó desligado: ele não vende nada, então não precisa permitir.
            return True
        except TempoEsgotado:
            return False

    def sair(self, recurso: str):
        """Sai da seção crítica e libera as respostas que estavam seguradas."""
        with self._cond:
            estava_dentro = self._estado.pop(recurso, (None,))[0] == DENTRO
            self._cond.notify_all()
        if estava_dentro:
            self.log(f"SAIU da seção crítica de {recurso}")
        self._lock_local(recurso).release()

    # ------------------------------------------------------------------
    # Lado de quem RECEBE o pedido
    # ------------------------------------------------------------------
    def ao_receber_pedido(self, recurso: str, origem: int, ts_pedido: int) -> int:
        self.relogio.atualizar(ts_pedido)

        def devo_esperar():
            atual = self._estado.get(recurso)
            if atual is None:
                return False                 # não quero o recurso
            estado, meu_ts = atual
            if estado == DENTRO:
                return True                  # estou usando o recurso
            # Os dois querem: menor (ts, id) tem prioridade.
            return (meu_ts, self.meu_id) < (ts_pedido, origem)

        with self._cond:
            if devo_esperar():
                self.log(f"segura a resposta para o nó {origem} sobre {recurso} "
                         f"(pedido dele ts={ts_pedido})")
                self._cond.wait_for(lambda: not devo_esperar())
                self.log(f"libera o nó {origem} para {recurso}")
        return self.relogio.tick()
