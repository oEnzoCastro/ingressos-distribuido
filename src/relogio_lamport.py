"""
RELÓGIO LÓGICO DE LAMPORT
=========================

Os relógios físicos das máquinas não são iguais. Por isso, o sistema não
usa a hora real para saber a ordem dos eventos. Ele usa um contador.

Regras:
  1. Antes de cada evento local ou envio de mensagem, o nó soma 1 ao contador.
  2. Toda mensagem leva o valor do contador.
  3. Quando o nó recebe uma mensagem, ele faz:
         contador = max(contador, valor_recebido) + 1

Resultado: se o evento A causou o evento B, então relogio(A) < relogio(B).

No sistema de ingressos, o relógio:
  - ordena os pedidos de acesso ao mesmo assento (Ricart-Agrawala);
  - ordena o log global de eventos de todos os nós.
"""

import threading


class RelogioLamport:
    def __init__(self):
        self._valor = 0
        self._lock = threading.Lock()  # vários threads usam o relógio

    def tick(self) -> int:
        """Evento local ou envio de mensagem: soma 1 e devolve o valor."""
        with self._lock:
            self._valor += 1
            return self._valor

    def atualizar(self, recebido: int) -> int:
        """Recebimento de mensagem: max(local, recebido) + 1."""
        with self._lock:
            self._valor = max(self._valor, recebido) + 1
            return self._valor

    @property
    def valor(self) -> int:
        with self._lock:
            return self._valor
