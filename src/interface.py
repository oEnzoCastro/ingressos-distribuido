"""Painel local para três nós e um cliente, todos em processos separados.

Executar na raiz do projeto: python src/interface.py
Antes do primeiro uso: pip install -r requirements.txt && python gerar_proto.py
Depois de atualizar o contrato .proto, gere os arquivos novamente e reabra a janela.
A interface usa os servidores e o cliente existentes, sem mudar seus protocolos.
"""

import importlib
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk


RAIZ = Path(__file__).resolve().parent.parent
ENDERECOS = [f"localhost:{50051 + i}" for i in range(3)]
PEERS = ",".join(f"{i + 1}={e}" for i, e in enumerate(ENDERECOS))


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
        janela.columnconfigure(0, weight=1)
        janela.rowconfigure(2, weight=1)

        barra = ttk.Frame(janela, padding=10)
        barra.grid(row=0, column=0, sticky="ew")
        ttk.Label(barra, text="Ingressos distribuídos", font=("Segoe UI", 16, "bold")).pack(side="left")
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
        self.parar_todos()
        self.limite_fechar = time.monotonic() + 5
        self._aguardar_encerramento()

    def _aguardar_encerramento(self):
        vivos = [t for t in self.terminais if t.processo and t.processo.poll() is None]
        if vivos and time.monotonic() < self.limite_fechar:
            self.janela.after(100, self._aguardar_encerramento)
        else:
            for terminal in vivos:
                try:
                    terminal.processo.kill()
                except OSError:
                    pass
            self.janela.destroy()


def main():
    janela = tk.Tk()
    Interface(janela)
    janela.mainloop()


if __name__ == "__main__":
    main()
