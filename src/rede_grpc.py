"""
REDE REAL COM gRPC (lado CLIENTE das chamadas entre nós)
========================================================

Aqui o nó chama funções em outros nós.

Como o gRPC funciona, em 3 passos:
  1. "canal" = conexão com o endereço do outro nó (ex.: "no2:50051").
  2. "stub"  = objeto que tem um método para cada rpc do arquivo .proto.
  3. Chamar  stub.PedirAcesso(mensagem)  executa a função no OUTRO nó
     e devolve a resposta, como uma função local.

Erros de rede viram as exceções da interface Rede:
  UNAVAILABLE        -> NoIndisponivel (não há conexão com o nó)
  qualquer outro     -> TempoEsgotado  (o nó não respondeu a tempo ou deu erro)

Só UNAVAILABLE conta como "nó desligado", porque a exclusão mútua trata um
nó desligado como permissão concedida. Um erro interno do outro nó NÃO
pode virar permissão: ele cancela a operação.
"""

import grpc

import ingressos_pb2 as pb
import ingressos_pb2_grpc as pb_grpc
from rede import NoIndisponivel, Rede, TempoEsgotado

TIMEOUT_CURTO = 2.0     # segundos, para mensagens simples
TIMEOUT_ACESSO = 15.0   # PedirAcesso pode esperar outro nó sair da seção crítica

# Quando uma conexão falha, o gRPC espera antes de tentar de novo, e essa
# espera cresce até 2 minutos. Enquanto isso, as chamadas falham na hora,
# mesmo que o outro nó já tenha voltado. Aqui a espera fica em no máximo 1 s,
# para o sistema perceber rápido que um nó voltou.
OPCOES_CANAL = [
    ("grpc.initial_reconnect_backoff_ms", 300),
    ("grpc.min_reconnect_backoff_ms", 300),
    ("grpc.max_reconnect_backoff_ms", 1000),
]


class RedeGrpc(Rede):
    def __init__(self, enderecos: dict):
        """enderecos = {id_do_no: "host:porta"}"""
        self._stubs = {
            no_id: pb_grpc.NoIngressosStub(grpc.insecure_channel(endereco, options=OPCOES_CANAL))
            for no_id, endereco in enderecos.items()
        }

    def _chamar(self, destino, metodo, mensagem, timeout=TIMEOUT_CURTO):
        funcao = getattr(self._stubs[destino], metodo)
        try:
            return funcao(mensagem, timeout=timeout)
        except grpc.RpcError as erro:
            if erro.code() == grpc.StatusCode.UNAVAILABLE:
                raise NoIndisponivel(destino) from erro
            raise TempoEsgotado(destino) from erro

    # ----- Exclusão mútua -----
    def pedir_acesso(self, destino, recurso, origem, relogio):
        r = self._chamar(destino, "PedirAcesso",
                         pb.PedidoAcesso(recurso=recurso, origem=origem, relogio=relogio),
                         timeout=TIMEOUT_ACESSO)
        venda = (r.vendido_para, r.vendido_no, r.vendido_relogio) if r.vendido_para else None
        return r.relogio, venda

    # ----- Replicação -----
    def replicar_venda(self, destino, assento, comprador, no_origem, relogio_venda, relogio):
        r = self._chamar(destino, "ReplicarVenda",
                         pb.Venda(assento=assento, comprador=comprador, no_origem=no_origem,
                                  relogio_venda=relogio_venda, relogio=relogio))
        return r.relogio

    def definir_vendas(self, destino, abertas, relogio):
        r = self._chamar(destino, "DefinirVendas",
                         pb.EstadoVendas(abertas=abertas, relogio=relogio))
        return r.relogio

    def obter_estado(self, destino):
        r = self._chamar(destino, "ObterEstado", pb.Vazio())
        vendidos = {v.assento: (v.comprador, v.no_origem, v.relogio_venda) for v in r.vendidos}
        return vendidos, r.vendas_abertas, r.relogio

    def alterar_vendas(self, destino, abertas, relogio):
        r = self._chamar(destino, "AlterarVendas",
                         pb.PedidoVendas(abertas=abertas, relogio=relogio))
        return r.sucesso, r.mensagem

    # ----- Eleição -----
    def eleicao(self, destino, origem, relogio):
        return self._chamar(destino, "Eleicao",
                            pb.MsgEleicao(origem=origem, relogio=relogio)).relogio

    def coordenador(self, destino, lider, relogio):
        return self._chamar(destino, "Coordenador",
                            pb.MsgCoordenador(lider=lider, relogio=relogio)).relogio

    def heartbeat(self, destino, origem, relogio):
        r = self._chamar(destino, "Heartbeat", pb.MsgHeartbeat(origem=origem, relogio=relogio))
        return r.lider, list(r.ativos), r.relogio
