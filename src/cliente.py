"""
CLIENTE DE TERMINAL (comprador)
===============================

Uso:
  python src/cliente.py localhost:50051 localhost:50052 localhost:50053

O nó 1 é o primeiro endereço, o nó 2 é o segundo, e assim por diante.
Digite "ajuda" para ver os comandos.
"""

import sys
import threading

import grpc

import ingressos_pb2 as pb
import ingressos_pb2_grpc as pb_grpc
from rede_grpc import OPCOES_CANAL   # reconexão rápida quando um nó volta

FILEIRAS = "ABCDE"
COLUNAS = 10

AJUDA = """
Comandos:
  status                      status de todos os nós (líder, ativos, relógio)
  mapa [n]                    mapa de assentos do nó n (padrão: 1)
  comprar <n> <assento> <nome>  compra pelo nó n. Ex.: comprar 2 A5 Maria
  corrida <assento>           TODOS os nós tentam vender o mesmo assento ao mesmo tempo
  abrir [n] | fechar [n]      pede ao nó n para abrir/fechar vendas (vai ao líder)
  log                         log de todos os nós, em ordem de Lamport
  ajuda | sair
"""


class Cliente:
    def __init__(self, enderecos):
        self.enderecos = enderecos
        self.stubs = [pb_grpc.NoIngressosStub(grpc.insecure_channel(e, options=OPCOES_CANAL))
                      for e in enderecos]

    def stub(self, n):
        return self.stubs[int(n) - 1]

    def chamar(self, n, metodo, msg, timeout=20):
        try:
            return getattr(self.stub(n), metodo)(msg, timeout=timeout)
        except grpc.RpcError as e:
            print(f"  nó {n} ({self.enderecos[int(n) - 1]}) não respondeu: {e.code().name}")
            return None

    # ------------------------------------------------------------------
    def status(self):
        print(f"  {'nó':<4}{'líder':<7}{'ativos':<14}{'Lamport':<9}{'vendas':<9}vendidos")
        for n in range(1, len(self.stubs) + 1):
            try:
                s = self.stub(n).ConsultarStatus(pb.Vazio(), timeout=2)
            except grpc.RpcError:
                print(f"  {n:<4}FORA DO AR")
                continue
            lider = s.lider or "?"
            marca = " *" if s.lider == s.id else ""
            print(f"  {s.id:<4}{str(lider) + marca:<7}{str(list(s.ativos)):<14}{s.relogio:<9}"
                  f"{'abertas' if s.vendas_abertas else 'fechadas':<9}"
                  f"{s.vendidos}/{s.total_assentos}")
        print("  (* = este nó é o líder)")

    def mapa(self, n="1"):
        r = self.chamar(n, "ConsultarMapa", pb.Vazio())
        if r is None:
            return
        vendidos = {v.assento: v for v in r.vendidos}
        print(f"  Mapa no nó {n} - vendas {'ABERTAS' if r.vendas_abertas else 'FECHADAS'}")
        print("      " + " ".join(f"{c:>2}" for c in range(1, COLUNAS + 1)))
        for f in FILEIRAS:
            linha = " ".join(" X" if f"{f}{c}" in vendidos else " ." for c in range(1, COLUNAS + 1))
            print(f"   {f}  {linha}")
        for v in r.vendidos:
            print(f"   {v.assento:<4} {v.comprador:<12} (vendido pelo nó {v.no_origem}, L={v.relogio_venda})")

    def comprar(self, n, assento, nome):
        r = self.chamar(n, "Comprar", pb.PedidoCompra(assento=assento, comprador=nome))
        if r:
            print(f"  [{'OK' if r.sucesso else 'RECUSADO'}] nó {r.no}: {r.mensagem}")

    def corrida(self, assento):
        """Dispara a mesma compra em todos os nós ao mesmo tempo."""
        barreira = threading.Barrier(len(self.stubs))
        resultados = {}

        def tentar(n):
            barreira.wait()
            resultados[n] = self.chamar(n, "Comprar", pb.PedidoCompra(assento=assento, comprador=f"Comprador-no{n}"))

        threads = [threading.Thread(target=tentar, args=(n,)) for n in range(1, len(self.stubs) + 1)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        for n in sorted(resultados):
            r = resultados[n]
            if r:
                print(f"  nó {n}: [{'OK' if r.sucesso else 'RECUSADO'}] {r.mensagem}")
        vencedores = sum(1 for r in resultados.values() if r and r.sucesso)
        print(f"  -> {vencedores} venda(s) do assento {assento.upper()}. O correto é no máximo 1.")

    def alterar(self, abrir, n="1"):
        r = self.chamar(n, "AlterarVendas", pb.PedidoVendas(abertas=abrir))
        if r:
            print(f"  [{'OK' if r.sucesso else 'ERRO'}] {r.mensagem}")

    def log(self):
        eventos = []
        for n in range(1, len(self.stubs) + 1):
            try:
                eventos += list(self.stub(n).ConsultarLog(pb.Vazio(), timeout=2).eventos)
            except grpc.RpcError:
                pass
        # Ordem total: primeiro o relógio de Lamport, depois o id do nó (desempate)
        for e in sorted(eventos, key=lambda e: (e.relogio, e.no))[-60:]:
            print(f"  L={e.relogio:>4}  nó {e.no}  {e.texto}")


def main():
    enderecos = sys.argv[1:] or ["localhost:50051", "localhost:50052", "localhost:50053"]
    c = Cliente(enderecos)
    print(f"Cliente conectado a {len(enderecos)} nós: {enderecos}")
    print(AJUDA)
    while True:
        try:
            partes = input("ingressos> ").split()
        except (EOFError, KeyboardInterrupt):
            break
        if not partes:
            continue
        cmd, args = partes[0].lower(), partes[1:]
        try:
            if cmd == "sair":
                break
            elif cmd == "ajuda":
                print(AJUDA)
            elif cmd == "status":
                c.status()
            elif cmd == "mapa":
                c.mapa(*args[:1])
            elif cmd == "comprar" and len(args) >= 3:
                c.comprar(args[0], args[1], " ".join(args[2:]))
            elif cmd == "corrida" and args:
                c.corrida(args[0])
            elif cmd in ("abrir", "fechar"):
                c.alterar(cmd == "abrir", *args[:1])
            elif cmd == "log":
                c.log()
            else:
                print("  comando inválido. Digite 'ajuda'.")
        except (IndexError, ValueError):
            print("  número de nó inválido.")


if __name__ == "__main__":
    main()
