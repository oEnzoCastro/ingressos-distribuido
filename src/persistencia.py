"""
PERSISTÊNCIA EM DISCO (tratamento de falhas, Entrega 2)
=======================================================

Problema: os dados ficavam só na memória. Se TODOS os nós caíssem ao
mesmo tempo, as vendas confirmadas se perdiam.

Solução: cada nó grava as vendas confirmadas em um arquivo JSON próprio
(por exemplo, dados/no_1.json). Ao iniciar, o nó lê esse arquivo antes de
falar com os outros nós.

Escrita atômica: o nó grava em um arquivo temporário e depois troca pelo
definitivo (os.replace). Se o nó cair no meio da gravação, o arquivo antigo
continua inteiro; nunca fica um arquivo pela metade.
"""

import json
import os


class Persistencia:
    def __init__(self, caminho):
        """caminho = arquivo JSON deste nó; None desliga a persistência."""
        self.caminho = caminho

    def carregar(self):
        """Devolve (vendidos, vendas_abertas). Sem arquivo: ({}, False)."""
        if not self.caminho or not os.path.exists(self.caminho):
            return {}, False
        with open(self.caminho, encoding="utf-8") as f:
            dados = json.load(f)
        vendidos = {a: tuple(v) for a, v in dados.get("vendidos", {}).items()}
        return vendidos, bool(dados.get("vendas_abertas", False))

    def salvar(self, vendidos, vendas_abertas):
        if not self.caminho:
            return
        pasta = os.path.dirname(self.caminho)
        if pasta:
            os.makedirs(pasta, exist_ok=True)
        temporario = self.caminho + ".tmp"
        with open(temporario, "w", encoding="utf-8") as f:
            json.dump({"vendidos": {a: list(v) for a, v in vendidos.items()},
                       "vendas_abertas": vendas_abertas}, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporario, self.caminho)
