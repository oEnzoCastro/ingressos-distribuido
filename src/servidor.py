"""
SERVIDOR gRPC DE UM NÓ (lado que RECEBE as chamadas)
====================================================

O "servicer" tem um método para cada rpc do arquivo .proto.
O gRPC chama esse método quando outro nó (ou o cliente) faz a chamada.
Cada método só converte a mensagem e chama a lógica em no_ingressos.py.

Como executar um nó:
  python src/servidor.py --id 1 --porta 50051 \
      --peers "1=localhost:50051,2=localhost:50052,3=localhost:50053"

Também é possível usar variáveis de ambiente (usadas pelo Docker):
  NO_ID, PORTA, PEERS, ATRASO_SECAO_CRITICA
"""

import argparse
import os
import signal
import sys
from concurrent import futures

import grpc

import ingressos_pb2 as pb
import ingressos_pb2_grpc as pb_grpc
from no_ingressos import ASSENTOS, NoIngressos
from rede_grpc import RedeGrpc


class ServicoNo(pb_grpc.NoIngressosServicer):
    def __init__(self, no: NoIngressos):
        self.no = no

    # ---------- Cliente ----------
    def Comprar(self, req, ctx):
        ok, msg, rel = self.no.comprar(req.assento, req.comprador)
        return pb.RespostaCompra(sucesso=ok, mensagem=msg, relogio=rel, no=self.no.meu_id)

    def ConsultarMapa(self, req, ctx):
        return self._estado()

    def ConsultarStatus(self, req, ctx):
        return pb.Status(**self.no.status())

    def ConsultarLog(self, req, ctx):
        return pb.Log(eventos=[pb.Evento(relogio=r, no=n, texto=t)
                               for r, n, t in self.no.eventos()])

    def AlterarVendas(self, req, ctx):
        ok, msg = self.no.alterar_vendas(req.abertas, req.relogio)
        return pb.RespostaSimples(sucesso=ok, mensagem=msg)

    # ---------- Exclusão mútua ----------
    def PedirAcesso(self, req, ctx):
        # Esta chamada pode "ficar parada" aqui até o nó liberar o assento.
        rel, venda = self.no.mutex.ao_receber_pedido(req.recurso, req.origem, req.relogio)
        if venda is None:
            return pb.Permissao(relogio=rel)
        comprador, no_origem, relogio_venda = venda
        return pb.Permissao(relogio=rel, vendido_para=comprador,
                            vendido_no=no_origem, vendido_relogio=relogio_venda)

    # ---------- Replicação ----------
    def ReplicarVenda(self, req, ctx):
        rel = self.no.ao_receber_venda(req.assento, req.comprador, req.no_origem,
                                       req.relogio_venda, req.relogio)
        return pb.Confirmacao(relogio=rel)

    def DefinirVendas(self, req, ctx):
        return pb.Confirmacao(relogio=self.no.ao_receber_definir_vendas(req.abertas, req.relogio))

    def ObterEstado(self, req, ctx):
        return self._estado()

    # ---------- Eleição ----------
    def Eleicao(self, req, ctx):
        return pb.Confirmacao(relogio=self.no.eleicao.ao_receber_eleicao(req.origem, req.relogio))

    def Coordenador(self, req, ctx):
        return pb.Confirmacao(relogio=self.no.eleicao.ao_receber_coordenador(req.lider, req.relogio))

    def Heartbeat(self, req, ctx):
        lider, ativos, rel = self.no.ao_receber_heartbeat(req.origem, req.relogio)
        return pb.RespostaHeartbeat(lider=lider, ativos=ativos, relogio=rel)

    # ---------- Auxiliar ----------
    def _estado(self):
        vendidos, abertas, rel = self.no.estado()
        return pb.Estado(
            vendidos=[pb.Venda(assento=a, comprador=c, no_origem=n, relogio_venda=r)
                      for a, (c, n, r) in sorted(vendidos.items(),
                                                 key=lambda x: ASSENTOS.index(x[0]))],
            vendas_abertas=abertas, relogio=rel)


def ler_peers(texto):
    """'1=host:porta,2=host:porta' -> {1: 'host:porta', 2: 'host:porta'}"""
    peers = {}
    for parte in texto.split(","):
        no_id, endereco = parte.strip().split("=")
        peers[int(no_id)] = endereco.strip()
    return peers


def main():
    p = argparse.ArgumentParser(description="Nó do sistema distribuído de ingressos")
    p.add_argument("--id", type=int, default=int(os.getenv("NO_ID", "0")))
    p.add_argument("--porta", type=int, default=int(os.getenv("PORTA", "50051")))
    p.add_argument("--peers", default=os.getenv("PEERS", ""),
                   help='todos os nós, incluindo este: "1=host:porta,2=host:porta"')
    p.add_argument("--atraso", type=float, default=float(os.getenv("ATRASO_SECAO_CRITICA", "0")),
                   help="segundos dentro da seção crítica (para ver a espera na demo)")
    args = p.parse_args()

    if not args.id or not args.peers:
        p.error("informe --id e --peers (ou NO_ID e PEERS)")

    peers = ler_peers(args.peers)
    outros = {i: e for i, e in peers.items() if i != args.id}

    no = NoIngressos(args.id, list(peers.keys()), RedeGrpc(outros),
                     atraso_secao_critica=args.atraso)

    # Muitos threads: PedirAcesso pode ficar esperando, e o servidor
    # precisa continuar atendendo outras chamadas enquanto isso.
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    pb_grpc.add_NoIngressosServicer_to_server(ServicoNo(no), servidor)
    servidor.add_insecure_port(f"0.0.0.0:{args.porta}")
    servidor.start()
    print(f"Nó {args.id} ouvindo na porta {args.porta}", flush=True)

    no.iniciar()

    def encerrar(*_):
        no.parar()
        servidor.stop(1)
        sys.exit(0)

    signal.signal(signal.SIGTERM, encerrar)
    signal.signal(signal.SIGINT, encerrar)
    servidor.wait_for_termination()


if __name__ == "__main__":
    main()
