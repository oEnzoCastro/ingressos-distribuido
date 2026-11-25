"""Painel local para três nós e um cliente, todos em processos separados.

Executar na raiz do projeto: python src/interface.py
Antes do primeiro uso: pip install -r requirements.txt && python gerar_proto.py
Depois de atualizar o contrato .proto, gere os arquivos novamente e reabra a janela.
A interface usa os servidores e o cliente existentes, sem mudar seus protocolos.
Na aba Desempenho, cada rodada inicia um cluster temporário com 1, 3 ou 5 nós
e mede 1.000 tentativas aleatórias em 500 assentos, sem tocar nos dados da operação.
"""

import importlib
import math
import os
from pathlib import Path
import queue
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import messagebox, scrolledtext, ttk


RAIZ = Path(__file__).resolve().parent.parent
ENDERECOS = [f"localhost:{50051 + i}" for i in range(3)]
PEERS = ",".join(f"{i + 1}={e}" for i, e in enumerate(ENDERECOS))
ASSENTOS_DESEMPENHO = tuple(f"{f}{c}" for f in "ABCDE" for c in range(1, 101))
COMPRAS_DESEMPENHO = 1000


class TesteCancelado(Exception):
    pass


class ExecutorDesempenho:
    """Benchmark com servidores reais, portas livres e dados próprios por rodada.

    A medição inclui só as compras: inicialização, eleição e verificação das
    réplicas ficam fora do tempo total. Todas as tentativas entram na latência.
    """

    def __init__(self, publicar):
        self.publicar = publicar
        self.cancelado = threading.Event()
        self._lock = threading.Lock()
        self._processos = []

    def _checar(self):
        if self.cancelado.is_set():
            raise TesteCancelado()

    def cancelar(self):
        self.cancelado.set()
        # Desliga os servidores para interromper também RPCs em andamento.
        threading.Thread(target=self._encerrar_processos, daemon=True).start()

    def _encerrar_processos(self):
        with self._lock:
            processos = list(self._processos)
        for processo in processos:
            if processo.poll() is None:
                try:
                    processo.terminate()
                except OSError:
                    pass
        for processo in processos:
            try:
                processo.wait(timeout=3)
            except subprocess.TimeoutExpired:
                processo.kill()
                processo.wait()

    def executar(self, cenarios, clientes, rodadas):
        resultados = []
        try:
            # Alternar os cenários reduz o efeito da ordem nas repetições.
            for rodada in range(1, rodadas + 1):
                # Um sorteio novo por rodada e por clique. Na comparação,
                # os cenários desta rodada compartilham a semente.
                semente = random.SystemRandom().getrandbits(64)
                ordem = cenarios if rodada % 2 else tuple(reversed(cenarios))
                for nos in ordem:
                    self._checar()
                    resultado = self._rodada(nos, clientes, rodada, semente)
                    resultados.append(resultado)
                    self.publicar("resultado", resultado)
            self.publicar("concluido", resultados)
        except TesteCancelado:
            self.publicar("cancelado", None)
        except Exception as erro:
            self.publicar("cancelado" if self.cancelado.is_set() else "erro", str(erro))
        finally:
            self._encerrar_processos()
            self.publicar("fim", None)

    def _rodada(self, nos, clientes, rodada, semente):
        grpc = importlib.import_module("grpc")
        pb = importlib.import_module("ingressos_pb2")
        pb_grpc = importlib.import_module("ingressos_pb2_grpc")
        self.publicar("progresso", f"Preparando {nos} nó(s), rodada {rodada}…")
        canais, sockets = [], []
        self._processos = []
        with tempfile.TemporaryDirectory(prefix="ingressos-benchmark-") as pasta:
            try:
                # Reserva todas as portas antes de iniciar os processos.
                for _ in range(nos):
                    reserva = socket.socket()
                    reserva.bind(("127.0.0.1", 0))
                    sockets.append(reserva)
                enderecos = [f"127.0.0.1:{s.getsockname()[1]}" for s in sockets]
                peers = ",".join(f"{i}={endereco}" for i, endereco in enumerate(enderecos, 1))
                for i, (endereco, reserva) in enumerate(zip(enderecos, sockets), 1):
                    porta = endereco.rsplit(":", 1)[1]
                    with self._lock:
                        self._checar()
                        reserva.close()
                        processo = subprocess.Popen([
                            sys.executable, "-u", str(Path(__file__).resolve()), "--no-desempenho",
                            "--id", str(i), "--porta", porta, "--peers", peers,
                            "--atraso", "0", "--dados", str(Path(pasta) / "dados"),
                        ], cwd=RAIZ, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                        self._processos.append(processo)
                    canais.append(grpc.insecure_channel(endereco, options=[
                        ("grpc.initial_reconnect_backoff_ms", 100),
                        ("grpc.min_reconnect_backoff_ms", 100),
                        ("grpc.max_reconnect_backoff_ms", 300),
                    ]))
                stubs = [pb_grpc.NoIngressosStub(canal) for canal in canais]

                # Só começa com eleição concluída e todos os nós prontos.
                prazo = time.monotonic() + 25
                while True:
                    self._checar()
                    if any(p.poll() is not None for p in self._processos):
                        raise RuntimeError("Um servidor de teste encerrou durante a inicialização.")
                    try:
                        status = [s.ConsultarStatus(pb.Vazio(), timeout=.5) for s in stubs]
                        if all(s.id == i and s.lider == nos and s.total_nos == nos
                               and s.total_assentos == len(ASSENTOS_DESEMPENHO)
                               and s.quorum == nos // 2 + 1 for i, s in enumerate(status, 1)):
                            abrir = stubs[-1].AlterarVendas(pb.PedidoVendas(abertas=True), timeout=3)
                            if abrir.sucesso and all(s.ConsultarStatus(pb.Vazio(), timeout=.5).vendas_abertas for s in stubs):
                                # Consultas também aquecem os canais de leitura.
                                if all(not s.ConsultarMapa(pb.Vazio(), timeout=.5).vendidos for s in stubs):
                                    break
                    except grpc.RpcError:
                        pass
                    if time.monotonic() >= prazo:
                        raise RuntimeError("Os servidores de teste não ficaram prontos em 25 segundos.")
                    self.cancelado.wait(.15)

                # Sorteio com reposição: cada tentativa pode disputar um assento
                # já escolhido. A semente da rodada é igual nos cenários comparados.
                assentos = random.Random(semente).choices(ASSENTOS_DESEMPENHO, k=COMPRAS_DESEMPENHO)

                def comprar(indice, assento):
                    self._checar()
                    comprador = f"Teste-{indice + 1:03d}"
                    inicio = time.perf_counter()
                    try:
                        resposta = stubs[indice % nos].Comprar(pb.PedidoCompra(
                            assento=assento, comprador=comprador), timeout=10)
                        return assento, comprador, "sucesso" if resposta.sucesso else "recusa", time.perf_counter() - inicio, resposta.mensagem
                    except grpc.RpcError as erro:
                        return assento, comprador, "erro", time.perf_counter() - inicio, erro.code().name

                respostas = []
                self.publicar("progresso", f"{nos} nó(s) · rodada {rodada} · 0/{COMPRAS_DESEMPENHO} compras")
                with ThreadPoolExecutor(max_workers=clientes) as pool:
                    inicio = time.perf_counter()
                    futuros = [pool.submit(comprar, i, a) for i, a in enumerate(assentos)]
                    try:
                        for futuro in as_completed(futuros):
                            self._checar()
                            respostas.append(futuro.result())
                            self.publicar("progresso", f"{nos} nó(s) · rodada {rodada} · {len(respostas)}/{COMPRAS_DESEMPENHO} compras")
                        duracao = time.perf_counter() - inicio
                    finally:
                        for futuro in futuros:
                            futuro.cancel()

                # Conferência imediata; não espera a anti-entropia esconder divergências.
                self._checar()
                mapas = [{v.assento: (v.comprador, v.no_origem, v.relogio_venda)
                          for v in s.ConsultarMapa(pb.Vazio(), timeout=3).vendidos} for s in stubs]
                esperados = {a: comprador for a, comprador, tipo, _, _ in respostas if tipo == "sucesso"}
                sucessos = sum(tipo == "sucesso" for _, _, tipo, _, _ in respostas)
                duplicadas = sucessos - len(esperados)
                iguais = all(mapa == mapas[0] for mapa in mapas)
                confirmacoes = all({a: v[0] for a, v in mapa.items()} == esperados for mapa in mapas)
                verificacao = []
                for indice, mapa in enumerate(mapas, 1):
                    faltando = sorted(set(esperados) - set(mapa))
                    extras = sorted(set(mapa) - set(esperados))
                    diferentes = sorted(a for a in set(esperados) & set(mapa) if mapa[a][0] != esperados[a])
                    metadados = sorted(a for a in set(mapas[0]) & set(mapa) if mapa[a] != mapas[0][a])
                    verificacao.append(f"Nó {indice}: {len(mapa)} vendas; faltando={faltando}; "
                                       f"extras={extras}; comprador diferente={diferentes}; "
                                       f"metadados diferentes do nó 1={metadados}")
                latencias = sorted(tempo * 1000 for _, _, _, tempo, _ in respostas)
                return {
                    "nos": nos, "rodada": rodada, "clientes": clientes, "tentativas": len(respostas),
                    "sucessos": sucessos,
                    "recusas": sum(tipo == "recusa" for _, _, tipo, _, _ in respostas),
                    "erros": sum(tipo == "erro" for _, _, tipo, _, _ in respostas),
                    "duplicadas": duplicadas, "assentos_sorteados": len(set(assentos)),
                    "semente": semente,
                    "duracao": duracao, "vazao": sucessos / duracao,
                    "tentativas_s": len(respostas) / duracao,
                    "media_ms": statistics.mean(latencias),
                    "p95_ms": latencias[math.ceil(.95 * len(latencias)) - 1],
                    "replicas": "Iguais" if iguais else "Divergentes",
                    "confirmacoes": "OK" if confirmacoes else "Divergem",
                    "latencias": latencias,
                    "verificacao": verificacao,
                    "falhas": [(a, tipo, msg) for a, _, tipo, _, msg in respostas if tipo != "sucesso"],
                }
            finally:
                self._encerrar_processos()
                for canal in canais:
                    canal.close()
                for reserva in sockets:
                    reserva.close()


class PaginaDesempenho(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent, padding=18)
        self.app = app
        self.eventos = queue.Queue()
        self.executor = None
        self.thread = None
        self.resultados = []
        self.columnconfigure(0, weight=1)
        self.rowconfigure(5, weight=1)
        topo = ttk.Frame(self)
        topo.grid(row=0, column=0, sticky="ew")
        ttk.Label(topo, text="Teste de desempenho", font=("Segoe UI", 20, "bold")).pack(side="left")
        ttk.Button(topo, text="Voltar à operação", command=lambda: app.paginas.select(0)).pack(side="right")
        explicacao = (
            f"{COMPRAS_DESEMPENHO} tentativas aleatórias entre {len(ASSENTOS_DESEMPENHO)} assentos de teste, com repetição e compradores diferentes. "
            "Clusters de 1, 3 e 5 nós usam quórum 1/1, 2/3 e 3/5; as compras são distribuídas em rodízio.\n"
            "Cada execução e rodada faz um novo sorteio. Em ‘Comparar todos’, os três cenários usam a mesma sequência. "
            "Cada rodada usa dados vazios e portas separadas. Atraso artificial: 0 s. "
            "Os nós e as vendas da operação são preservados."
        )
        descricao = ttk.Label(self, text=explicacao, justify="left", wraplength=900)
        descricao.grid(row=1, column=0, sticky="ew", pady=12)
        self.bind("<Configure>", lambda e: descricao.configure(wraplength=max(300, e.width - 40)))
        opcoes = ttk.Frame(self)
        opcoes.grid(row=2, column=0, sticky="ew")
        self.clientes = tk.IntVar(value=5)
        self.rodadas = tk.IntVar(value=1)
        ttk.Label(opcoes, text="Clientes simultâneos:").pack(side="left")
        self.campo_clientes = ttk.Spinbox(opcoes, from_=1, to=10, textvariable=self.clientes, width=4)
        self.campo_clientes.pack(side="left", padx=(6, 18))
        ttk.Label(opcoes, text="Rodadas por cenário:").pack(side="left")
        self.campo_rodadas = ttk.Spinbox(opcoes, from_=1, to=10, textvariable=self.rodadas, width=4)
        self.campo_rodadas.pack(side="left", padx=6)
        ttk.Label(opcoes, text="Sugestão: repetir 3 vezes para comparar.").pack(side="left", padx=12)
        acoes = ttk.Frame(self)
        acoes.grid(row=3, column=0, sticky="ew", pady=12)
        self.botoes = []
        cenarios_disponiveis = [
            (f"{COMPRAS_DESEMPENHO} tentativas · {nos} {'nó' if nos == 1 else 'nós'}", (nos,))
            for nos in (1, 3, 5)
        ]
        for titulo, cenarios in [*cenarios_disponiveis, ("Comparar todos", (1, 3, 5))]:
            botao = ttk.Button(acoes, text=titulo, command=lambda c=cenarios: self.iniciar(c))
            botao.pack(side="left", padx=(0, 8))
            self.botoes.append(botao)
        self.cancelar_btn = ttk.Button(acoes, text="Cancelar teste", state="disabled", command=self.cancelar)
        self.cancelar_btn.pack(side="left")
        self.limpar_btn = ttk.Button(acoes, text="Limpar resultados", command=self.limpar_resultados)
        self.limpar_btn.pack(side="right")
        self.progresso = tk.StringVar(value="Escolha um cenário ou compare todos.")
        ttk.Label(self, textvariable=self.progresso).grid(row=4, column=0, sticky="w", pady=(0, 8))
        tabela = ttk.Frame(self)
        tabela.grid(row=5, column=0, sticky="nsew")
        tabela.columnconfigure(0, weight=1)
        tabela.rowconfigure(0, weight=1)
        colunas = [
            ("cenario", "Cenário / rodada", 135), ("clientes", "Clientes", 65),
            ("sucessos", f"Sucessos / {COMPRAS_DESEMPENHO}", 110),
            ("recusas", "Recusas", 70), ("erros", "Erros RPC", 80),
            ("duplicadas", "Vendas duplicadas", 120),
            ("tempo", "Tempo (s)", 90), ("tentativas_s", "Tentativas/s", 100), ("vazao", "Vendas/s", 90),
            ("media", "Média (ms)", 95), ("p95", "p95 (ms)", 95),
            ("replicas", "Réplicas", 100), ("confirmacoes", "Dados × respostas", 125),
        ]
        self.tabela = ttk.Treeview(tabela, columns=[c[0] for c in colunas], show="headings", height=9)
        for chave, titulo, largura in colunas:
            self.tabela.heading(chave, text=titulo)
            self.tabela.column(chave, width=largura, minwidth=largura, anchor="center")
        self.tabela.grid(row=0, column=0, sticky="nsew")
        vertical = ttk.Scrollbar(tabela, orient="vertical", command=self.tabela.yview)
        vertical.grid(row=0, column=1, sticky="ns")
        horizontal = ttk.Scrollbar(tabela, orient="horizontal", command=self.tabela.xview)
        horizontal.grid(row=1, column=0, sticky="ew")
        self.tabela.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        self.resumo = tk.StringVar(value="Os resultados aparecerão aqui. As rodadas anteriores permanecem na tabela.")
        ttk.Label(self, textvariable=self.resumo, wraplength=900, justify="left").grid(row=6, column=0, sticky="ew", pady=12)
        self.detalhes = scrolledtext.ScrolledText(self, height=5, wrap="word", state="disabled")
        self.detalhes.grid(row=7, column=0, sticky="ew")
        ttk.Label(self, text=(
            f"Tempo: só as {COMPRAS_DESEMPENHO} tentativas, sem inicialização e conferência. Vendas/s: sucessos ÷ tempo. "
            "Latência: duração de cada RPC, incluindo falhas e sem espera na fila do cliente; p95 usa o percentil por posto.\n"
            "Recusas por assento vendido são esperadas. Duplicadas: confirmações além da primeira para o mesmo assento. "
            "Réplicas são comparadas imediatamente após o teste. "
            "Todos os processos rodam nesta máquina; evite outras compras durante a medição."
        ), wraplength=900, justify="left").grid(row=8, column=0, sticky="ew", pady=(10, 0))
        self.after(100, self._consumir)

    def iniciar(self, cenarios):
        if self.thread and self.thread.is_alive():
            return
        try:
            clientes, rodadas = self.clientes.get(), self.rodadas.get()
            if not (1 <= clientes <= 10 and 1 <= rodadas <= 10):
                raise ValueError()
        except (ValueError, tk.TclError):
            messagebox.showinfo("Parâmetros", "Use de 1 a 10 clientes e de 1 a 10 rodadas.")
            return
        if not self.app.dependencias_ok():
            return
        self.resultados = []
        self.resumo.set(f"Executando com {clientes} cliente(s) simultâneo(s) e {rodadas} rodada(s) por cenário.")
        self._ocupado(True)
        self.executor = ExecutorDesempenho(lambda tipo, dado: self.eventos.put((tipo, dado)))
        self.thread = threading.Thread(target=self.executor.executar, args=(cenarios, clientes, rodadas), daemon=True)
        self.thread.start()

    def _ocupado(self, ocupado):
        for botao in [*self.botoes, self.limpar_btn, self.campo_clientes, self.campo_rodadas]:
            botao.configure(state="disabled" if ocupado else "normal")
        self.cancelar_btn.configure(state="normal" if ocupado else "disabled")

    def limpar_resultados(self):
        if self.limpar_btn.instate(["disabled"]) or (self.thread and self.thread.is_alive()):
            return
        itens = self.tabela.get_children()
        if itens:
            self.tabela.delete(*itens)
        self.resultados.clear()
        self.detalhes.configure(state="normal")
        self.detalhes.delete("1.0", "end")
        self.detalhes.configure(state="disabled")
        self.resumo.set("Os resultados aparecerão aqui. As rodadas anteriores permanecem na tabela.")
        self.progresso.set("Escolha um cenário ou compare todos.")

    def cancelar(self):
        if self.executor:
            self.executor.cancelar()
            self.progresso.set("Cancelando e encerrando os servidores de teste…")

    def _consumir(self):
        while True:
            try:
                tipo, dado = self.eventos.get_nowait()
            except queue.Empty:
                break
            if tipo == "progresso":
                self.progresso.set(dado)
            elif tipo == "resultado":
                self.resultados.append(dado)
                self.tabela.insert("", "end", values=(
                    f"{dado['nos']} nó(s) · #{dado['rodada']}", dado["clientes"], dado["sucessos"], dado["recusas"], dado["erros"],
                    dado["duplicadas"], f"{dado['duracao']:.3f}", f"{dado['tentativas_s']:.2f}", f"{dado['vazao']:.2f}", f"{dado['media_ms']:.2f}",
                    f"{dado['p95_ms']:.2f}", dado["replicas"], dado["confirmacoes"],
                ))
                self.detalhes.configure(state="normal")
                self.detalhes.insert("end", f"{dado['nos']} nó(s), rodada {dado['rodada']}, {dado['clientes']} clientes: "
                                     f"{dado['tentativas']} tentativas; {dado['sucessos']} confirmações; "
                                     f"{dado['recusas']} recusas; {dado['duplicadas']} vendas duplicadas; "
                                     f"{dado['assentos_sorteados']} assentos sorteados; semente {dado['semente']}.\n")
                for assento, falha, mensagem in dado["falhas"]:
                    self.detalhes.insert("end", f"  {assento}: {falha} — {mensagem}\n")
                for linha in dado["verificacao"]:
                    self.detalhes.insert("end", f"  {linha}\n")
                self.detalhes.see("end")
                self.detalhes.configure(state="disabled")
            elif tipo == "concluido":
                self.progresso.set("Teste concluído. Servidores de teste encerrando.")
                partes = []
                taxas = {}
                for nos in sorted({r["nos"] for r in dado}):
                    grupo = [r for r in dado if r["nos"] == nos]
                    tempo = sum(r["duracao"] for r in grupo)
                    sucessos = sum(r["sucessos"] for r in grupo)
                    taxas[nos] = sum(r["tentativas"] for r in grupo) / tempo
                    recusas = sum(r["recusas"] for r in grupo)
                    partes.append(f"{nos} nó(s): {len(grupo)} rodada(s), média de {tempo / len(grupo):.3f} s/rodada; "
                                  f"{taxas[nos]:.2f} tentativas/s; {sucessos / tempo:.2f} vendas/s; "
                                  f"{sucessos} sucessos e {recusas} recusas")
                if 1 in taxas and taxas[1] > 0:
                    for nos in sorted(taxas):
                        if nos != 1:
                            partes.append(f"Tentativas/s com {nos} nós ÷ 1 nó: {taxas[nos] / taxas[1]:.2f}×")
                if any(r["duplicadas"] or r["erros"] or r["replicas"] != "Iguais"
                       or r["confirmacoes"] != "OK" for r in dado):
                    partes.append("Há falhas ou divergências: confira a tabela e os detalhes antes de comparar a velocidade.")
                self.resumo.set("\n".join(partes))
            elif tipo == "cancelado":
                self.progresso.set("Teste cancelado. Rodadas incompletas não entram nos resultados.")
            elif tipo == "erro":
                self.progresso.set(f"Teste interrompido: {dado}")
            elif tipo == "fim":
                self._ocupado(False)
        if not self.app.fechando.is_set():
            self.after(100, self._consumir)


class Terminal:
    def __init__(self, app, parent, nome, comando, cliente=False):
        self.app = app
        self.comando = comando
        self.cliente = cliente
        self.processo = None
        self.parando = False
        self.frame = ttk.LabelFrame(parent, text=nome, padding=8)
        self.frame.rowconfigure(1, weight=1)
        self.frame.columnconfigure(0, weight=1)
        barra = ttk.Frame(self.frame)
        barra.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.estado = tk.StringVar(value="Parado")
        ttk.Label(barra, textvariable=self.estado).pack(side="left")
        self.iniciar_btn = ttk.Button(barra, text="Iniciar", command=self.iniciar)
        self.iniciar_btn.pack(side="right")
        self.parar_btn = ttk.Button(barra, text="Parar", command=self.parar, state="disabled")
        self.parar_btn.pack(side="right", padx=4)
        ttk.Button(barra, text="Limpar log", command=self.limpar).pack(side="right", padx=4)
        self.texto = scrolledtext.ScrolledText(
            self.frame, height=12, width=60, bg="#111827", fg="#dbeafe",
            insertbackground="white", font=("Consolas", 10), wrap="word",
            state="disabled",
        )
        self.texto.grid(row=1, column=0, sticky="nsew")
        self.escrever("Pronto para iniciar.\n")

    def limpar(self):
        self.texto.configure(state="normal")
        self.texto.delete("1.0", "end")
        self.texto.configure(state="disabled")

    def escrever(self, texto):
        acompanhar = self.texto.yview()[1] >= 0.99
        self.texto.configure(state="normal")
        self.texto.insert("end", texto)
        # Limita o histórico para sessões de demonstração longas.
        linhas = int(self.texto.index("end-1c").split(".")[0])
        if linhas > 2500:
            self.texto.delete("1.0", f"{linhas - 2000}.0")
        if acompanhar:
            self.texto.see("end")
        self.texto.configure(state="disabled")

    def iniciar(self):
        if self.parando or (self.processo and self.processo.poll() is None):
            return
        if not self.app.dependencias_ok():
            return
        ambiente = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        try:
            self.processo = subprocess.Popen(
                [sys.executable, "-u", *self.comando], cwd=RAIZ, env=ambiente,
                stdin=subprocess.PIPE if self.cliente else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError as erro:
            self.escrever(f"Não foi possível iniciar: {erro}\n")
            return
        self.estado.set("Processo iniciado")
        self.iniciar_btn.configure(state="disabled")
        self.parar_btn.configure(state="normal")
        self.escrever("\n--- Processo iniciado ---\n")
        threading.Thread(target=self._ler, args=(self.processo,), daemon=True).start()

    def _ler(self, processo):
        # Lê também o prompt sem quebra de linha do cliente.
        try:
            while caractere := processo.stdout.read(1):
                self.app.eventos.put(("texto", self, processo, caractere))
        finally:
            codigo = processo.wait()
            processo.stdout.close()
            if processo.stdin:
                processo.stdin.close()
            self.app.eventos.put(("fim", self, processo, codigo))

    def enviar(self, comando):
        if not self.processo or self.processo.poll() is not None or self.parando:
            self.escrever("\nInicie o cliente antes de enviar comandos.\n")
            return
        try:
            self.processo.stdin.write(comando + "\n")
            self.processo.stdin.flush()
            self.escrever(comando + "\n")
        except (OSError, ValueError) as erro:
            self.escrever(f"\nNão foi possível enviar: {erro}\n")

    def parar(self):
        if not self.processo or self.processo.poll() is not None or self.parando:
            return
        self.parando = True
        self.estado.set("Encerrando…")
        self.parar_btn.configure(state="disabled")
        threading.Thread(target=self._encerrar, args=(self.processo,), daemon=True).start()

    @staticmethod
    def _encerrar(processo):
        try:
            processo.terminate()
            try:
                processo.wait(timeout=3)
            except subprocess.TimeoutExpired:
                processo.kill()
                processo.wait()
        except OSError:
            pass  # O processo pode ter terminado entre poll() e terminate().

    def terminou(self, processo, codigo):
        if processo is not self.processo:
            return
        self.parando = False
        self.estado.set(f"Parado (código {codigo})")
        self.iniciar_btn.configure(state="normal")
        self.parar_btn.configure(state="disabled")
        self.escrever(f"\n--- Processo encerrado: {codigo} ---\n")


class Interface:
    def __init__(self, janela):
        self.janela = janela
        self.eventos = queue.Queue()
        self.fechando = threading.Event()
        self.no_mapa = 1  # Cópia simples: o worker não acessa widgets Tk.
        self.rpc = None
        janela.title("Ingressos distribuídos | Controle dos processos")
        janela.geometry("1380x900")
        janela.minsize(1000, 720)
        janela.protocol("WM_DELETE_WINDOW", self.fechar)
        self.paginas = ttk.Notebook(janela)
        self.paginas.pack(fill="both", expand=True)
        janela = ttk.Frame(self.paginas)
        self.paginas.add(janela, text="Operação")
        self.desempenho = PaginaDesempenho(self.paginas, self)
        self.paginas.add(self.desempenho, text="Desempenho")
        janela.columnconfigure(0, weight=1)
        janela.rowconfigure(2, weight=1)

        barra = ttk.Frame(janela, padding=10)
        barra.grid(row=0, column=0, sticky="ew")
        ttk.Label(barra, text="Ingressos distribuídos", font=("Segoe UI", 16, "bold")).pack(side="left")
        ttk.Button(barra, text="Teste de desempenho", command=lambda: self.paginas.select(self.desempenho)).pack(side="right", padx=4)
        ttk.Button(barra, text="Iniciar todos", command=self.iniciar_todos).pack(side="right", padx=4)
        ttk.Button(barra, text="Parar todos", command=self.parar_todos).pack(side="right", padx=4)
        ttk.Label(barra, text="Para reiniciar um nó: Parar → Iniciar").pack(side="right", padx=12)

        mapa = ttk.LabelFrame(janela, text="Mapa de assentos — clique para selecionar", padding=8)
        mapa.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 8))
        esquerda = ttk.Frame(mapa)
        esquerda.pack(side="left")
        self.assentos = {}
        for linha, fileira in enumerate("ABCDE"):
            for coluna in range(10):
                nome = f"{fileira}{coluna + 1}"
                botao = tk.Button(esquerda, text=nome, width=4, relief="flat", bg="#d1d5db",
                                  command=lambda a=nome: self.assento.set(a))
                botao.grid(row=linha, column=coluna, padx=2, pady=2)
                self.assentos[nome] = botao
        direita = ttk.Frame(mapa, padding=(20, 0))
        direita.pack(side="left", fill="both", expand=True)
        direita.bind("<Configure>", lambda evento: [
            filho.configure(wraplength=max(180, evento.width - 40))
            for filho in direita.winfo_children()
        ])
        self.resumo = tk.StringVar(value="Inicie os processos. As informações serão atualizadas automaticamente.")
        ttk.Label(direita, textvariable=self.resumo, justify="left").pack(anchor="w")
        self.quorum = tk.StringVar(value="Quórum: aguardando consulta aos nós.")
        ttk.Label(direita, textvariable=self.quorum, justify="left").pack(anchor="w")
        ttk.Label(direita, text="Verde: livre  •  Vermelho: vendido  •  Cinza: sem consulta atual").pack(anchor="w", pady=5)
        ttk.Label(direita, text="Mapa é a cópia local; a compra valida o quórum. Dados são preservados ao reiniciar.").pack(anchor="w")
        self.detalhe = tk.StringVar(value="")
        ttk.Label(direita, textvariable=self.detalhe).pack(anchor="w")

        grade = ttk.Frame(janela, padding=(10, 0, 10, 10))
        grade.grid(row=2, column=0, sticky="nsew")
        for i in range(2):
            grade.rowconfigure(i, weight=1)
            grade.columnconfigure(i, weight=1)
        self.terminais = []
        for i in range(1, 4):
            terminal = Terminal(self, grade, f"Nó {i} · {ENDERECOS[i - 1]}", [
                "src/servidor.py", "--id", str(i), "--porta", str(50050 + i),
                "--peers", PEERS, "--atraso", "3",
            ])
            self.terminais.append(terminal)
        self.cliente = Terminal(self, grade, "Cliente · comandos e resultados",
                                ["src/cliente.py", *ENDERECOS], cliente=True)
        self.terminais.append(self.cliente)
        for i, terminal in enumerate(self.terminais):
            terminal.frame.grid(row=i // 2, column=i % 2, sticky="nsew", padx=4, pady=4)
        self._controles_cliente()
        self.vendidos = None
        self.assento.trace_add("write", lambda *_: self._detalhar())
        self.janela.after(80, self._consumir)
        threading.Thread(target=self._monitorar, daemon=True).start()

    def _controles_cliente(self):
        painel = ttk.Frame(self.cliente.frame)
        painel.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.no = tk.StringVar(value="1")
        self.assento = tk.StringVar(value="A1")
        self.comprador = tk.StringVar(value="Maria")
        ttk.Label(painel, text="Nó / mapa").pack(side="left")
        seletor = ttk.Combobox(painel, textvariable=self.no, values=("1", "2", "3"), width=3, state="readonly")
        seletor.pack(side="left", padx=4)
        seletor.bind("<<ComboboxSelected>>", self._trocar_no)
        ttk.Entry(painel, textvariable=self.assento, width=5).pack(side="left", padx=4)
        ttk.Entry(painel, textvariable=self.comprador, width=17).pack(side="left", padx=4)
        ttk.Button(painel, text="Comprar", command=self._comprar).pack(side="left", padx=4)
        ttk.Button(painel, text="Corrida", command=lambda: self.cliente.enviar(f"corrida {self.assento.get()}")).pack(side="left")
        atalhos = ttk.Frame(self.cliente.frame)
        atalhos.grid(row=3, column=0, sticky="ew", pady=4)
        for titulo, comando in [("Status", "status"), ("Mapa", "mapa"), ("Abrir vendas", "abrir"),
                                ("Fechar vendas", "fechar"), ("Log global", "log"), ("Ajuda", "ajuda")]:
            ttk.Button(atalhos, text=titulo, command=lambda c=comando: self.cliente.enviar(
                f"{c} {self.no.get()}" if c in ("mapa", "abrir", "fechar") else c
            )).pack(side="left", padx=2)
        entrada = ttk.Frame(self.cliente.frame)
        entrada.grid(row=4, column=0, sticky="ew")
        entrada.columnconfigure(0, weight=1)
        self.comando = ttk.Entry(entrada)
        self.comando.grid(row=0, column=0, sticky="ew")
        self.comando.bind("<Return>", self._enviar)
        ttk.Button(entrada, text="Enviar", command=self._enviar).grid(row=0, column=1, padx=4)

    def _trocar_no(self, _evento=None):
        self.no_mapa = int(self.no.get())
        self._pintar_mapa(None)

    def _comprar(self):
        if not self.comprador.get().strip() or not self.assento.get().strip():
            messagebox.showinfo("Compra", "Preencha o assento e o nome do comprador.")
            return
        self.cliente.enviar(f"comprar {self.no.get()} {self.assento.get().strip()} {self.comprador.get().strip()}")

    def _enviar(self, _evento=None):
        comando = self.comando.get().strip()
        if comando:
            self.cliente.enviar(comando)
            self.comando.delete(0, "end")

    def dependencias_ok(self):
        try:
            importlib.import_module("grpc")
            pb = importlib.import_module("ingressos_pb2")
            importlib.import_module("ingressos_pb2_grpc")
            self._validar_protocolo(pb)
            return True
        except (ImportError, RuntimeError) as erro:
            messagebox.showerror("Preparação necessária", (
                "Execute estes comandos na raiz do projeto, usando o mesmo Python da interface:\n\n"
                f'"{sys.executable}" -m pip install -r requirements.txt\n'
                f'"{sys.executable}" gerar_proto.py\n\n'
                f"Depois, feche e reabra a interface.\n\nDetalhe: {erro}"
            ))
            return False

    @staticmethod
    def _validar_protocolo(pb):
        """Detecta stubs antigos que importam, mas ainda não conhecem o quórum."""
        campos = pb.Status.DESCRIPTOR.fields_by_name
        servico = pb.DESCRIPTOR.services_by_name.get("NoIngressos")
        metodos = servico.methods_by_name if servico else {}
        if not {"quorum", "total_nos"}.issubset(campos) or not {
            "ReservarAssento", "ConfirmarVenda", "CancelarReserva",
        }.issubset(metodos):
            raise RuntimeError("Protocolo gerado desatualizado para a versão com quórum.")

    def iniciar_todos(self):
        if self.dependencias_ok():
            for terminal in self.terminais:
                terminal.iniciar()

    def parar_todos(self):
        for terminal in self.terminais:
            terminal.parar()

    def _monitorar(self):
        canais = []
        try:
            while not self.fechando.is_set():
                try:
                    if self.rpc is None:
                        grpc = importlib.import_module("grpc")
                        pb = importlib.import_module("ingressos_pb2")
                        pb_grpc = importlib.import_module("ingressos_pb2_grpc")
                        self._validar_protocolo(pb)
                        canais = [grpc.insecure_channel(e, options=[
                            ("grpc.max_reconnect_backoff_ms", 1000),
                        ]) for e in ENDERECOS]
                        self.rpc = (grpc, pb, [pb_grpc.NoIngressosStub(c) for c in canais])
                    grpc, pb, stubs = self.rpc
                    linhas = []
                    configuracoes = set()
                    respondendo = 0
                    for i, stub in enumerate(stubs, 1):
                        try:
                            s = stub.ConsultarStatus(pb.Vazio(), timeout=0.5)
                            respondendo += 1
                            papel = "LÍDER" if s.lider == s.id else f"líder: {s.lider or '?'}"
                            if s.quorum > 0 and s.total_nos > 0:
                                configuracoes.add((s.quorum, s.total_nos))
                                regra = f"quórum {s.quorum}/{s.total_nos}"
                            else:
                                regra = "servidor sem informação de quórum"
                            linhas.append(f"Nó {i}: {papel} | vendas {'abertas' if s.vendas_abertas else 'fechadas'} | {s.vendidos}/{s.total_assentos} | {regra}")
                        except grpc.RpcError:
                            linhas.append(f"Nó {i}: sem resposta")
                    # Responder ao painel não comprova comunicação entre os nós.
                    # Só o protocolo da compra pode determinar se há quórum.
                    if len(configuracoes) == 1:
                        minimo, total = next(iter(configuracoes))
                        mensagem = f"Mínimo para vender: {minimo} de {total} nós. Respondendo ao painel: {respondendo}."
                    elif configuracoes:
                        mensagem = "Os nós consultados informam configurações de quórum diferentes."
                    else:
                        mensagem = f"Quórum não informado. Respondendo ao painel: {respondendo}."
                    self.eventos.put(("quorum", mensagem, None, None))
                    escolhido = self.no_mapa
                    try:
                        estado = stubs[escolhido - 1].ConsultarMapa(pb.Vazio(), timeout=0.5)
                        vendidos = {v.assento: v.comprador for v in estado.vendidos}
                    except grpc.RpcError:
                        vendidos = None
                    self.eventos.put(("status", "\n".join(linhas), escolhido, vendidos))
                except (ImportError, RuntimeError):
                    self.eventos.put(("quorum", "Prepare as dependências e execute gerar_proto.py; depois reabra a interface.", None, None))
                self.fechando.wait(2)
        finally:
            for canal in canais:
                canal.close()

    def _pintar_mapa(self, vendidos):
        self.vendidos = vendidos
        for assento, botao in self.assentos.items():
            cor = "#d1d5db" if vendidos is None else "#fca5a5" if assento in vendidos else "#86efac"
            botao.configure(bg=cor, activebackground=cor)
        self._detalhar()

    def _detalhar(self):
        assento = self.assento.get().strip().upper()
        if self.vendidos is None:
            texto = f"Nó {self.no_mapa}: sem consulta atual."
        elif assento not in self.assentos:
            texto = "Selecione um assento de A1 a E10."
        elif assento in self.vendidos:
            texto = f"Nó {self.no_mapa} · {assento}: vendido para {self.vendidos[assento]}"
        else:
            texto = f"Nó {self.no_mapa} · {assento}: livre"
        self.detalhe.set(texto)

    def _consumir(self):
        textos = {}
        for _ in range(12000):
            try:
                tipo, alvo, processo, dado = self.eventos.get_nowait()
            except queue.Empty:
                break
            if tipo == "texto":
                textos.setdefault(alvo, []).append(dado)
            elif tipo == "fim":
                if alvo in textos:
                    alvo.escrever("".join(textos.pop(alvo)))
                alvo.terminou(processo, dado)
            elif tipo == "status":
                self.resumo.set(alvo)
                if processo == self.no_mapa:
                    self._pintar_mapa(dado)
            elif tipo == "quorum":
                self.quorum.set(alvo)
        for terminal, partes in textos.items():
            terminal.escrever("".join(partes))
        if not self.fechando.is_set():
            self.janela.after(80, self._consumir)

    def fechar(self):
        self.fechando.set()
        self.desempenho.cancelar()
        self.parar_todos()
        self.limite_fechar = time.monotonic() + 5
        self._aguardar_encerramento()

    def _aguardar_encerramento(self):
        vivos = [t for t in self.terminais if t.processo and t.processo.poll() is None]
        teste_ativo = self.desempenho.thread and self.desempenho.thread.is_alive()
        if (vivos or teste_ativo) and time.monotonic() < self.limite_fechar:
            self.janela.after(100, self._aguardar_encerramento)
        else:
            if teste_ativo:
                # A thread já recebeu cancelamento; não deixa subprocessos ao sair.
                with self.desempenho.executor._lock:
                    for processo in self.desempenho.executor._processos:
                        if processo.poll() is None:
                            try:
                                processo.kill()
                            except OSError:
                                pass
            for terminal in vivos:
                try:
                    terminal.processo.kill()
                except OSError:
                    pass
            self.janela.destroy()


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--no-desempenho":
        # Só o subprocesso de teste recebe o mapa ampliado. A operação
        # normal continua com seus 50 assentos e seus próprios dados.
        sys.argv.pop(1)
        from no_ingressos import ASSENTOS
        from servidor import main as iniciar_servidor

        ASSENTOS[:] = ASSENTOS_DESEMPENHO
        iniciar_servidor()
        return
    janela = tk.Tk()
    Interface(janela)
    janela.mainloop()


if __name__ == "__main__":
    main()
