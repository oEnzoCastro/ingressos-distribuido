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
from rede import NoIndisponivel             # noqa: E402


def arquivo(pasta, no_id):
    return os.path.join(pasta, f"no_{no_id}.json") if pasta else None


def criar_cluster(n=3, atraso=0.0, abertas=True, iniciar=True, pasta_dados=None,
                  reserva_expira=10.0):
    rede = RedeMemoria()
    ids = list(range(1, n + 1))
    for i in ids:
        rede.nos[i] = NoIngressos(i, ids, rede.visao(i), intervalo_heartbeat=0.2,
                                  limite_inativo=1.0, espera_coordenador=0.5,
                                  atraso_secao_critica=atraso,
                                  vendas_abertas=abertas, verbose=False,
                                  arquivo_dados=arquivo(pasta_dados, i),
                                  reserva_expira=reserva_expira)
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
    for no, ordem in ((rede.nos[1], (a, b)), (rede.nos[2], (b, a))):
        with no._lock_dados:
            for venda in ordem:
                no._mesclar_venda("C3", venda, "réplica")
    assert rede.nos[1].vendidos["C3"] == rede.nos[2].vendidos["C3"] == a


# ----------------------------------------------------------------------
# Entrega 2: quórum (controle de réplicas) e persistência
# ----------------------------------------------------------------------
def test_no_isolado_nao_vende():
    """Um nó isolado de todos não tem maioria: recusa vender.
    O lado com a maioria (nós 2 e 3) continua vendendo."""
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    rede.isolar(1)
    ok, msg, _ = rede.nos[1].comprar("E1", "Isolada")
    assert not ok and "quórum" in msg.lower(), msg
    assert "E1" not in rede.nos[1].vendidos and "E1" not in rede.nos[1].reservas
    ok, msg, _ = rede.nos[2].comprar("E1", "Maioria")
    assert ok, msg
    assert rede.nos[3].vendidos["E1"][0] == "Maioria"


def test_minoria_sobrevivente_nao_vende():
    """Com 2 de 3 nós desligados, o nó que sobrou não tem quórum."""
    rede = criar_cluster(3)
    desligar(rede, 2)
    desligar(rede, 3)
    ok, msg, _ = rede.nos[1].comprar("E2", "Ana")
    assert not ok and "quórum" in msg.lower(), msg


def test_particao_assimetrica_sem_venda_dupla():
    """Os nós 1 e 2 não se falam, mas os dois falam com o 3. Sem quórum,
    os dois poderiam vender o mesmo assento ao mesmo tempo. Com quórum, o
    nó 3 só aceita uma reserva: no máximo uma venda por assento."""
    rede = criar_cluster(3, atraso=0.02)
    rede.bloqueados |= {(1, 2), (2, 1)}
    for assento in ASSENTOS[20:30]:
        resultado = {}
        barreira = threading.Barrier(2)

        def tentar(i):
            barreira.wait()
            resultado[i] = rede.nos[i].comprar(assento, f"c{i}")[0]

        ts = [threading.Thread(target=tentar, args=(i,)) for i in (1, 2)]
        [t.start() for t in ts]
        [t.join(15) for t in ts]
        assert sum(resultado.values()) <= 1, (assento, resultado)
        vencedores = [i for i, ok in resultado.items() if ok]
        if vencedores:
            dono = f"c{vencedores[0]}"
            assert rede.nos[3].vendidos[assento][0] == dono
            assert rede.nos[vencedores[0]].vendidos[assento][0] == dono


def test_reserva_de_no_que_caiu_expira():
    """Se quem reservou cai antes da fase 2, a reserva expira sozinha."""
    rede = criar_cluster(3, reserva_expira=0.3)
    for i in (2, 3):
        rede.nos[i].ao_receber_reserva("D9", ("Fantasma", 1, 5), relogio=5)
    ok, msg, _ = rede.nos[2].comprar("D9", "Bia")
    assert not ok, "vendeu um assento reservado"
    time.sleep(0.4)
    ok, msg, _ = rede.nos[2].comprar("D9", "Bia")
    assert ok, msg


def test_persistencia_sobrevive_a_queda_de_todos():
    """Todos os nós caem; um nó que volta sozinho lê as vendas do disco."""
    import tempfile
    pasta = tempfile.mkdtemp()
    rede = criar_cluster(3, pasta_dados=pasta)
    assert rede.nos[1].comprar("A9", "Duda")[0]
    for i in (1, 2, 3):
        desligar(rede, i)
    novo = RedeMemoria()
    novo.nos[2] = NoIngressos(2, [1, 2, 3], novo.visao(2), verbose=False,
                              arquivo_dados=arquivo(pasta, 2))
    novo.nos[2].iniciar()
    novo.nos[2].parar()
    assert novo.nos[2].vendidos["A9"][0] == "Duda"
    assert novo.nos[2].vendas_abertas


def test_sincronizacao_periodica_corrige_confirmacao_perdida():
    """O nó 3 fica isolado durante uma venda e perde a confirmação. Quando a
    rede volta, a sincronização periódica entrega a venda ao nó 3 sozinha,
    sem ninguém pedir aquele assento de novo."""
    rede = criar_cluster(3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    rede.isolar(3)
    assert esperar(lambda: rede.nos[2].eleicao.lider == 2)
    assert rede.nos[1].comprar("B7", "Caio")[0]
    assert "B7" not in rede.nos[3].vendidos
    rede.bloqueados.clear()
    assert esperar(lambda: rede.nos[3].vendidos.get("B7", ("",))[0] == "Caio", limite=8)



# ----------------------------------------------------------------------
# Casos de erro encontrados na revisão da Entrega 2
# ----------------------------------------------------------------------
def test_confirmacao_perdida_e_vendedor_caido_nao_revende():
    """A confirmação não chega a ninguém e o vendedor cai. As reservas expiram,
    mas o assento NÃO pode ser vendido de novo: fica bloqueado em dúvida."""
    rede = criar_cluster(3, reserva_expira=0.3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})

    def falha(*_):
        raise NoIndisponivel(0)
    rede.nos[1].rede.confirmar_venda = falha
    assert rede.nos[1].comprar("A1", "Maria")[0]
    desligar(rede, 1)
    time.sleep(0.5)
    ok, msg, _ = rede.nos[2].comprar("A1", "João")
    assert not ok, "venda dupla: " + msg


def test_reserva_em_duvida_resolvida_quando_vendedor_volta():
    """Mesmo caso; quando o vendedor responde, os outros nós adotam a venda."""
    rede = criar_cluster(3, reserva_expira=0.3)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    original = rede.nos[1].rede.confirmar_venda

    def falha(*_):
        raise NoIndisponivel(0)
    rede.nos[1].rede.confirmar_venda = falha
    assert rede.nos[1].comprar("A2", "Maria")[0]
    rede.nos[1].rede.confirmar_venda = original
    time.sleep(0.4)
    ok, msg, _ = rede.nos[2].comprar("A2", "João")
    assert not ok and "Maria" in msg, msg
    assert rede.nos[2].vendidos["A2"][0] == "Maria"


def test_reserva_sobrevive_a_reinicio_do_no():
    """O nó 3 vota na Maria e reinicia antes da confirmação. Ele não pode
    esquecer o voto e votar no João (o voto fica gravado em disco)."""
    import tempfile
    pasta = tempfile.mkdtemp()
    rede = criar_cluster(3, pasta_dados=pasta)
    assert esperar(lambda: set(lideres(rede).values()) == {3})
    rede.bloqueados |= {(1, 2), (2, 1)}
    assert rede.nos[3].ao_receber_reserva("B2", ("Maria", 1, 999), 1)[0]
    desligar(rede, 3)
    rede.desligados.discard(3)
    rede.nos[3] = NoIngressos(3, [1, 2, 3], rede.visao(3), intervalo_heartbeat=0.2,
                              limite_inativo=1.0, espera_coordenador=0.5, verbose=False,
                              arquivo_dados=arquivo(pasta, 3))
    rede.nos[3].iniciar()
    ok, msg, _ = rede.nos[2].comprar("B2", "João")
    assert not ok, "o nó reiniciado votou duas vezes: " + msg


def test_tentativa_antiga_do_mesmo_vendedor_nao_bloqueia():
    rede = criar_cluster(3)
    for i in (2, 3):
        rede.nos[i].ao_receber_reserva("C3", ("Bia", 1, 5), 5)
    ok, msg, _ = rede.nos[1].comprar("C3", "Bia")
    assert ok, msg


def test_arquivo_corrompido_nao_impede_o_no_de_subir():
    import tempfile
    pasta = tempfile.mkdtemp()
    with open(arquivo(pasta, 1), "w") as f:
        f.write("{corrompido")
    rede = RedeMemoria()
    rede.nos[1] = NoIngressos(1, [1, 2, 3], rede.visao(1), verbose=False,
                              arquivo_dados=arquivo(pasta, 1))
    rede.nos[1].iniciar()
    rede.nos[1].parar()
    assert rede.nos[1].vendidos == {}


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
