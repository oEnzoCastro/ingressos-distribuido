"""
PERSISTÊNCIA EM DISCO (tratamento de falhas, Entrega 2)
=======================================================

Problema: os dados ficavam só na memória. Se TODOS os nós caíssem ao
mesmo tempo, as vendas confirmadas se perdiam.

Solução: cada nó grava em um arquivo JSON próprio (por exemplo,
dados/no_1.json):
  - as vendas confirmadas;
  - se as vendas estão abertas;
  - as reservas que ele aceitou (o "voto" do quórum). Se o nó reiniciar
    entre a reserva e a confirmação, ele não pode esquecer em quem votou,
    senão votaria de novo em outra venda do mesmo assento.

Ao iniciar, o nó lê esse arquivo antes de falar com os outros nós.

Escrita atômica: o nó grava em um arquivo temporário e depois troca pelo
definitivo (os.replace). Se o nó cair no meio da gravação, o arquivo antigo
continua inteiro; nunca fica um arquivo pela metade.
"""

import json
import os


class ArquivoCorrompido(Exception):
    """O arquivo de dados existe, mas não é um JSON válido."""


class Persistencia:
    def __init__(self, caminho):
        """caminho = arquivo JSON deste nó; None desliga a persistência."""
        self.caminho = caminho

    def carregar(self):
        """Devolve (vendidos, vendas_abertas, reservas).
        Sem arquivo: ({}, False, {}). Arquivo inválido: ArquivoCorrompido."""
        if not self.caminho or not os.path.exists(self.caminho):
            return {}, False, {}
        try:
            with open(self.caminho, encoding="utf-8") as f:
                dados = json.load(f)
            vendidos = {a: tuple(v) for a, v in dados.get("vendidos", {}).items()}
            reservas = {a: (tuple(v), float(expira))
                        for a, (v, expira) in dados.get("reservas", {}).items()}
            return vendidos, bool(dados.get("vendas_abertas", False)), reservas
        except (ValueError, TypeError, AttributeError) as erro:
            raise ArquivoCorrompido(f"{self.caminho}: {erro}") from erro

    def salvar(self, vendidos, vendas_abertas, reservas):
        if not self.caminho:
            return
        pasta = os.path.dirname(self.caminho)
        if pasta:
            os.makedirs(pasta, exist_ok=True)
        temporario = self.caminho + ".tmp"
        with open(temporario, "w", encoding="utf-8") as f:
            json.dump({"vendidos": {a: list(v) for a, v in vendidos.items()},
                       "vendas_abertas": vendas_abertas,
                       "reservas": {a: [list(v), expira] for a, (v, expira) in reservas.items()}},
                      f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporario, self.caminho)
