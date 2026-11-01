"""
Gera ingressos_pb2.py e ingressos_pb2_grpc.py a partir do arquivo .proto.
Execute depois de mudar proto/ingressos.proto:   python gerar_proto.py
"""
import os
import sys

from grpc_tools import protoc

raiz = os.path.dirname(os.path.abspath(__file__))
codigo = protoc.main([
    "grpc_tools.protoc",
    f"-I{os.path.join(raiz, 'proto')}",
    f"--python_out={os.path.join(raiz, 'src')}",
    f"--grpc_python_out={os.path.join(raiz, 'src')}",
    os.path.join(raiz, "proto", "ingressos.proto"),
])
print("Arquivos gerados em src/" if codigo == 0 else "Erro ao gerar os arquivos")
sys.exit(codigo)
