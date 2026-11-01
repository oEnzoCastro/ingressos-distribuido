"""
INTERFACE DE REDE
=================

Os algoritmos (Bully, Ricart-Agrawala, replicação) não falam com o gRPC
diretamente. Eles usam esta interface.

Por quê?
  - O código dos algoritmos fica limpo e fácil de explicar.
  - A implementação real usa gRPC (arquivo rede_grpc.py).
  - Os testes usam uma rede simulada em memória (tests/rede_memoria.py).

Toda função recebe o id do nó de destino e o relógio de Lamport.
Toda função devolve o relógio de Lamport do nó que respondeu.
"""


class NoIndisponivel(Exception):
    """O nó de destino não responde (desligado ou fora da rede)."""


class TempoEsgotado(Exception):
    """O nó não respondeu a tempo, ou respondeu com erro.
    Não prova que o nó está desligado, então a operação deve ser cancelada."""


class Rede:
    # ----- Exclusão mútua -----
    def pedir_acesso(self, destino: int, recurso: str, origem: int, relogio: int):
        """Devolve (relogio, info). info = o que o destino sabe sobre o recurso
        (para um assento: (comprador, no_origem, relogio_venda)) ou None."""
        raise NotImplementedError

    # ----- Replicação -----
    def replicar_venda(self, destino: int, assento: str, comprador: str,
                       no_origem: int, relogio_venda: int, relogio: int) -> int:
        raise NotImplementedError

    def definir_vendas(self, destino: int, abertas: bool, relogio: int) -> int:
        raise NotImplementedError

    def obter_estado(self, destino: int):
        """Devolve (vendidos, vendas_abertas, relogio).
        vendidos = {assento: (comprador, no_origem, relogio)}"""
        raise NotImplementedError

    def alterar_vendas(self, destino: int, abertas: bool, relogio: int):
        """Repassa ao líder o pedido de abrir/fechar. Devolve (sucesso, mensagem)."""
        raise NotImplementedError

    # ----- Eleição -----
    def eleicao(self, destino: int, origem: int, relogio: int) -> int:
        raise NotImplementedError

    def coordenador(self, destino: int, lider: int, relogio: int) -> int:
        raise NotImplementedError

    def heartbeat(self, destino: int, origem: int, relogio: int):
        """Devolve (lider, ativos, relogio)."""
        raise NotImplementedError
