"""
Testes dos algoritmos com a rede simulada.
Executar:  python -m pytest tests -v     (ou)     python tests/test_algoritmos.py
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from rede_memoria import RedeMemoria          # noqa: E402
from no_ingressos import NoIngressos, ASSENTOS  # noqa: E402
from relogio_lamport import RelogioLamport   # noqa: E402


def criar_cluster(n=3, atraso=0.0, abertas=True, iniciar=True):
    rede = RedeMemoria()
    ids = list(range(1, n + 1))
    for i in ids:
        rede.nos[i] = NoIngressos(i, ids, rede.visao(i), intervalo_heartbeat=0.2,
                                  limite_inativo=1.0, espera_coordenador=0.5,
                                  atraso_secao_critica=atraso,
                                  vendas_abertas=abertas, verbose=False)
    if iniciar:
        for i in ids:
            rede.nos[i].iniciar()
    return rede


def esperar(condicao, limite=5.0):
    fim = time.time() + limite
    while time.time() < fim:
        if condicao():
            return True
        time.sleep(0.05)
    return False


def lideres(rede):
    return {i: no.eleicao.lider for i, no in rede.nos.items() if i not in rede.desligados}


def desligar(rede, i):
    rede.desligados.add(i)
    rede.nos[i].parar()


# ----------------------------------------------------------------------
def test_lamport():
    r = RelogioLamport()
    assert r.tick() == 1
    assert r.tick() == 2
    assert r.atualizar(10) == 11      # max(2, 10) + 1
    assert r.atualizar(3) == 12       # max(11, 3) + 1


def test_corrida_pelo_mesmo_assento():
    """Os 3 nós tentam vender A5 ao mesmo tempo: só um consegue."""
    rede = criar_cluster(3, atraso=0.05)
    resultados = {}
    barreira = threading.Barrier(3)

    def tentar(i):
        barreira.wait()
        resultados[i] = rede.nos[i].comprar("A5", f"Comprador{i}")

    ts = [threading.Thread(target=tentar, args=(i,)) for i in (1, 2, 3)]
    [t.start() for t in ts]
    [t.join(10) for t in ts]

    sucessos = [i for i, r in resultados.items() if r[0]]
    assert len(sucessos) == 1, resultados
    donos = {no.vendidos["A5"][0] for no in rede.nos.values()}
    assert donos == {f"Comprador{sucessos[0]}"}, "réplicas diferentes"


def test_muitas_compras_concorrentes_sem_venda_dupla():
    rede = criar_cluster(3)
    assentos = ASSENTOS[:8]
    vencedores = []
    lock = threading.Lock()

    def cliente(no_id, k):
        for a in assentos:
            ok, _, _ = rede.nos[no_id].comprar(a, f"c{no_id}-{k}")
            if ok:
                with lock:
                    vencedores.append(a)

    ts = [threading.Thread(target=cliente, args=(i, k)) for i in (1, 2, 3) for k in range(3)]
    [t.start() for t in ts]
    [t.join(30) for t in ts]

    assert sorted(vencedores) == sorted(assentos), "algum assento vendido 2x ou não vendido"
    mapas = [no.vendidos for no in rede.nos.values()]
    assert mapas[0] == mapas[1] == mapas[2], "réplicas inconsistentes"


def test_eleicao_inicial_escolhe_maior_id():
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})


def test_falha_do_lider_e_volta():
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})

    desligar(rede, 3)
    assert esperar(lambda: set(lideres(rede).values()) == {2}), lideres(rede)

    # O sistema continua vendendo com um nó a menos
    ok, msg, _ = rede.nos[1].comprar("B1", "Ana")
    assert ok, msg
    assert rede.nos[2].vendidos["B1"][0] == "Ana"

    # O nó 3 volta: copia o estado e retoma a liderança (Bully)
    rede.desligados.discard(3)
    rede.nos[3] = NoIngressos(3, [1, 2, 3], rede.visao(3), intervalo_heartbeat=0.2,
                              limite_inativo=1.0, espera_coordenador=0.5, verbose=False)
    rede.nos[3].iniciar()
    assert rede.nos[3].vendidos["B1"][0] == "Ana", "nó que voltou não copiou o estado"
    assert esperar(lambda: set(lideres(rede).values()) == {3}), lideres(rede)


def test_abrir_vendas_passa_pelo_lider():
    rede = criar_cluster(3, abertas=False)
    assert esperar(lambda: set(lideres(rede).values()) == {3})

    ok, _, _ = rede.nos[1].comprar("C1", "Bia")
    assert not ok, "vendeu com as vendas fechadas"

    ok, msg = rede.nos[1].alterar_vendas(True)   # nó 1 repassa ao líder 3
    assert ok and "nó 3" in msg, msg
    assert all(no.vendas_abertas for no in rede.nos.values())
    assert rede.nos[1].comprar("C1", "Bia")[0]

    # Lamport: o repasse no nó 1 vem ANTES da decisão do líder
    repasse = [e[0] for e in rede.nos[1].eventos() if "repassa" in e[2]][0]
    decisao = [e[0] for e in rede.nos[3].eventos() if "LÍDER decide" in e[2]][0]
    assert repasse < decisao, (repasse, decisao)


def test_lider_conhece_nos_ativos():
    rede = criar_cluster(3)
    assert esperar(lambda: rede.nos[3].ativos == [1, 2, 3])
    desligar(rede, 1)
    assert esperar(lambda: rede.nos[3].ativos == [2, 3]), rede.nos[3].ativos


def test_log_ordenado_por_lamport():
    rede = criar_cluster(3)
    rede.nos[1].comprar("D1", "Caio")
    rede.nos[2].comprar("D2", "Duda")
    venda1 = rede.nos[1].vendidos["D1"][2]
    rep_no2 = [e for e in rede.nos[2].eventos() if "D1 vendido" in e[2]][0][0]
    assert rep_no2 > venda1, "réplica deveria ter relógio maior que a venda"


def test_lider_novo_nao_lista_no_morto():
    rede = criar_cluster(3)
    assert esperar(lambda: rede.nos[3].ativos == [1, 2, 3])
    desligar(rede, 3)
    assert esperar(lambda: rede.nos[2].eleicao.lider == 2)
    assert 3 not in rede.nos[2].ativos, rede.nos[2].ativos


def test_dois_lideres_se_corrigem():
    """Caso visto no teste real: o nó 3 volta, mas o nó 2 ainda não
    consegue falar com ele (só o caminho 3 -> 2 funciona). O nó 2 se
    declara líder. Quando o caminho volta, o sistema deve ter UM líder: o 3."""
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    desligar(rede, 3)
    assert esperar(lambda: set(lideres(rede).values()) == {2})

    rede.bloqueados.add((2, 3))
    rede.desligados.discard(3)
    rede.nos[3] = NoIngressos(3, [1, 2, 3], rede.visao(3), intervalo_heartbeat=0.2,
                              limite_inativo=1.0, espera_coordenador=0.5, verbose=False)
    rede.nos[3].iniciar()
    time.sleep(1.5)                       # tempo para o problema acontecer
    rede.bloqueados.clear()               # a rede se recupera
    assert esperar(lambda: set(lideres(rede).values()) == {3}, limite=8), lideres(rede)


def test_replica_perdida_nao_causa_venda_dupla():
    """Caso achado na revisão: o nó 1 vende A1 sem alcançar o nó 3 por um
    instante. Depois, o nó 3 tenta vender A1: ele deve descobrir a venda
    pela resposta da permissão e recusar."""
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    rede.bloqueados.add((1, 3))
    assert rede.nos[1].comprar("A1", "Maria")[0]
    rede.bloqueados.clear()

    ok, msg, _ = rede.nos[3].comprar("A1", "João")
    assert not ok, "venda dupla: " + msg
    donos = {i: n.vendidos["A1"][0] for i, n in rede.nos.items()}
    assert set(donos.values()) == {"Maria"}, donos


def test_conflito_resolvido_pela_ordem_de_lamport():
    """Se duas vendas do mesmo assento se encontram, todas as cópias ficam
    com a mesma: a de menor (relogio_venda, no_origem), em qualquer ordem."""
    rede = criar_cluster(3, iniciar=False)
    a, b = ("Maria", 1, 40), ("João", 3, 42)
    rede.nos[1].ao_receber_venda("C3", *a, relogio=50)
    rede.nos[1].ao_receber_venda("C3", *b, relogio=51)
    rede.nos[2].ao_receber_venda("C3", *b, relogio=50)
    rede.nos[2].ao_receber_venda("C3", *a, relogio=51)
    assert rede.nos[1].vendidos["C3"] == rede.nos[2].vendidos["C3"] == a


if __name__ == "__main__":
    import inspect
    falhas = 0
    for nome, f in list(globals().items()):
        if nome.startswith("test_") and inspect.isfunction(f):
            try:
                f()
                print(f"OK     {nome}")
            except AssertionError as e:
                falhas += 1
                print(f"FALHOU {nome}: {e}")
    sys.exit(1 if falhas else 0)
