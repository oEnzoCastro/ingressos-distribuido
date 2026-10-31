"""
ELEIÇÃO DE LÍDER - ALGORITMO BULLY ("valentão")
===============================================

Cada nó tem um id numérico. O nó ativo com o MAIOR id é o líder.

Como funciona:
  1. Um nó percebe que o líder não responde (ou acabou de iniciar).
  2. Ele envia "Eleicao" para todos os nós com id MAIOR que o dele.
  3. Se nenhum nó maior responde, ele vira o líder e envia
     "Coordenador" para todos os nós.
  4. Se algum nó maior responde, esse nó maior assume a eleição.
     O nó menor espera a mensagem "Coordenador". Se ela não chega a
     tempo, ele começa uma nova eleição.
  5. Quando um nó recebe "Eleicao" de um nó menor, ele responde OK e
     começa a sua própria eleição.

Quando um nó com id maior volta a funcionar, ele faz uma eleição e
"toma" a liderança. Por isso o nome "valentão".

Papel do líder no sistema de ingressos:
  - Abre e fecha as vendas para todos os nós.
  - Mantém a lista de nós ativos (pelos heartbeats).
"""

import threading

from rede import NoIndisponivel, TempoEsgotado


class EleicaoBully:
    def __init__(self, meu_id, todos_ids, rede, relogio, log,
                 ao_mudar_lider=None, espera_coordenador=3.0):
        self.meu_id = meu_id
        self.todos_ids = sorted(todos_ids)
        self.rede = rede
        self.relogio = relogio
        self.log = log
        self.ao_mudar_lider = ao_mudar_lider or (lambda lider: None)
        self.espera_coordenador = espera_coordenador

        self.lider = None
        self._em_eleicao = False
        self._lock = threading.Lock()
        self._coordenador_chegou = threading.Event()

    def _definir_lider(self, lider):
        mudou = self.lider != lider
        self.lider = lider
        if mudou:
            self.log(f"novo líder: nó {lider}")
            self.ao_mudar_lider(lider)

    def esquecer_lider(self):
        self.lider = None

    # ------------------------------------------------------------------
    def iniciar_eleicao(self):
        with self._lock:
            if self._em_eleicao:
                return                       # já existe uma eleição neste nó
            self._em_eleicao = True
            self._coordenador_chegou.clear()

        try:
            while True:
                maiores = [i for i in self.todos_ids if i > self.meu_id]
                self.log(f"inicia eleição; envia Eleicao para {maiores or 'ninguém'}")

                alguem_respondeu = False
                for destino in maiores:
                    try:
                        r = self.rede.eleicao(destino, self.meu_id, self.relogio.tick())
                        self.relogio.atualizar(r)
                        alguem_respondeu = True
                    except (NoIndisponivel, TempoEsgotado):
                        pass

                if not alguem_respondeu:
                    self._virar_lider()
                    return

                # Um nó maior está vivo. Ele vai anunciar o resultado.
                if self._coordenador_chegou.wait(self.espera_coordenador):
                    return
                self.log("nenhum Coordenador chegou a tempo; repete a eleição")
        finally:
            with self._lock:
                self._em_eleicao = False

    def _virar_lider(self):
        self._definir_lider(self.meu_id)
        for destino in self.todos_ids:
            if destino == self.meu_id:
                continue
            try:
                r = self.rede.coordenador(destino, self.meu_id, self.relogio.tick())
                self.relogio.atualizar(r)
            except (NoIndisponivel, TempoEsgotado):
                pass

    # ------------------------------------------------------------------
    # Mensagens recebidas
    # ------------------------------------------------------------------
    def ao_receber_eleicao(self, origem: int, relogio: int) -> int:
        self.relogio.atualizar(relogio)
        self.log(f"recebeu Eleicao do nó {origem}; responde OK e assume a eleição")
        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
        return self.relogio.tick()

    def ao_receber_coordenador(self, lider: int, relogio: int) -> int:
        self.relogio.atualizar(relogio)
        self._definir_lider(lider)
        self._coordenador_chegou.set()
        if lider < self.meu_id:
            # Um nó menor se declarou líder, mas eu estou vivo: "valentão".
            threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
        return self.relogio.tick()
