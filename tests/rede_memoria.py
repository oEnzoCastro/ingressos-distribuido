"""
Rede simulada em memória, só para os testes.
Ela chama os métodos dos outros nós diretamente, sem gRPC.

Falhas que dá para simular:
  - nó desligado:        rede.desligados.add(3)
  - link de um só lado:  rede.bloqueados.add((2, 3))   # 2 não alcança 3, mas 3 alcança 2
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from rede import NoIndisponivel, Rede  # noqa: E402


class RedeMemoria:
    def __init__(self):
        self.nos = {}
        self.desligados = set()
        self.bloqueados = set()

    def visao(self, origem):
        """A rede vista pelo nó 'origem'."""
        return _Visao(self, origem)


class _Visao(Rede):
    def __init__(self, rede, origem):
        self.r = rede
        self.origem = origem

    def _no(self, destino):
        r = self.r
        if destino in r.desligados or destino not in r.nos or \
           (self.origem, destino) in r.bloqueados:
            raise NoIndisponivel(destino)
        return r.nos[destino]

    def pedir_acesso(self, destino, recurso, origem, relogio):
        return self._no(destino).mutex.ao_receber_pedido(recurso, origem, relogio)

    def replicar_venda(self, destino, assento, comprador, no_origem, relogio_venda, relogio):
        return self._no(destino).ao_receber_venda(assento, comprador, no_origem,
                                                  relogio_venda, relogio)

    def definir_vendas(self, destino, abertas, relogio):
        return self._no(destino).ao_receber_definir_vendas(abertas, relogio)

    def obter_estado(self, destino):
        return self._no(destino).estado()

    def alterar_vendas(self, destino, abertas, relogio):
        return self._no(destino).alterar_vendas(abertas, relogio)

    def eleicao(self, destino, origem, relogio):
        return self._no(destino).eleicao.ao_receber_eleicao(origem, relogio)

    def coordenador(self, destino, lider, relogio):
        return self._no(destino).eleicao.ao_receber_coordenador(lider, relogio)

    def heartbeat(self, destino, origem, relogio):
        return self._no(destino).ao_receber_heartbeat(origem, relogio)
