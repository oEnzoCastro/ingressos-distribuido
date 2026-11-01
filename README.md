# Sistema distribuído de venda de ingressos

**Tema 7 – Banco de Dados Distribuído**, aplicado à venda de ingressos.

Vários servidores (nós) guardam uma cópia do mapa de assentos e vendem ao mesmo
tempo. O sistema garante que **nenhum assento é vendido duas vezes**, mesmo com
compras simultâneas em nós diferentes, e **continua funcionando quando um nó cai**.

## Requisitos da Entrega 1

| Requisito | Onde está | Para que serve no sistema |
|---|---|---|
| **RPC (gRPC)** | `proto/ingressos.proto`, `src/servidor.py`, `src/rede_grpc.py` | Toda a comunicação entre nós e entre cliente e nó |
| **Eleição de líder (Bully)** | `src/eleicao_bully.py` | Escolhe o coordenador, que abre/fecha as vendas e mantém a lista de nós ativos |
| **Exclusão mútua (Ricart-Agrawala)** | `src/exclusao_mutua.py` | Só um nó vende um assento por vez (um recurso por assento) |
| **Relógio lógico (Lamport)** | `src/relogio_lamport.py` | Desempata pedidos simultâneos e ordena o log global |

Extra: replicação do mapa de assentos e cópia do estado quando um nó volta
(`src/no_ingressos.py`). Isso já prepara a Entrega 2.

## Estrutura

```
proto/ingressos.proto   contrato gRPC (mensagens e funções)
src/relogio_lamport.py  relógio de Lamport
src/exclusao_mutua.py   Ricart-Agrawala
src/eleicao_bully.py    Bully + papel do líder
src/no_ingressos.py     lógica do nó: compra, réplica, heartbeat
src/rede.py             interface de rede usada pelos algoritmos
src/rede_grpc.py        implementação da interface com gRPC (lado cliente)
src/servidor.py         servidor gRPC de um nó (lado servidor)
src/cliente.py          cliente de terminal (comprador)
tests/                  testes dos algoritmos com rede simulada (sem gRPC)
```

Os algoritmos não dependem do gRPC: eles usam a interface `Rede`. Assim, cada
algoritmo fica em um arquivo curto e pode ser testado sem rede.

## Como executar

### Opção 1 – Docker (recomendado)

Precisa do Docker Desktop.

```bash
docker compose up --build              # sobe os nós 1, 2 e 3 (deixe este terminal aberto)
docker compose run --rm cliente        # em outro terminal: abre o cliente
```

### Opção 2 – Sem Docker (Windows, Linux ou Mac)

```bash
pip install -r requirements.txt
python gerar_proto.py
```

Abra 3 terminais, um para cada nó:

```bash
python src/servidor.py --id 1 --porta 50051 --peers "1=localhost:50051,2=localhost:50052,3=localhost:50053"
python src/servidor.py --id 2 --porta 50052 --peers "1=localhost:50051,2=localhost:50052,3=localhost:50053"
python src/servidor.py --id 3 --porta 50053 --peers "1=localhost:50051,2=localhost:50052,3=localhost:50053"
```

E um quarto terminal para o cliente:

```bash
python src/cliente.py localhost:50051 localhost:50052 localhost:50053
```

Use `--atraso 1` nos nós para que a espera da exclusão mútua fique visível.

### Opção 3 – Máquinas diferentes na rede local

O mesmo código. Troque `localhost` pelo IP de cada máquina em `--peers` e
libere as portas no firewall. Exemplo na máquina do nó 2:

```bash
python src/servidor.py --id 2 --porta 50051 --peers "1=192.168.0.10:50051,2=192.168.0.11:50051,3=192.168.0.12:50051"
```

## Roteiro da demonstração

No cliente:

1. `status`: os 3 nós escolheram o **nó 3 como líder** (maior id).
2. `comprar 1 A1 Maria`: **recusado**, porque as vendas começam fechadas.
3. `abrir 1`: o nó 1 **repassa ao líder**, e o líder abre as vendas para todos.
4. `corrida A5`: os 3 nós tentam vender o A5 ao mesmo tempo. **Só um consegue.**
5. `log`: mostra os eventos em ordem de Lamport, com quem segurou a resposta,
   quem entrou na seção crítica e as réplicas.
6. `mapa 2`: o nó 2 tem a mesma cópia dos dados.
7. Derrube o líder: `docker compose stop no3` (ou Ctrl+C no terminal do nó 3).
8. `status`: depois de alguns segundos, o **nó 2 é o novo líder**.
9. `comprar 1 B1 Ana`: as vendas **continuam** com 2 nós.
10. Religue: `docker compose start no3`. O nó 3 **copia o estado** e **retoma a
    liderança** (comportamento "valentão" do Bully).

## Testes

```bash
python tests/test_algoritmos.py
```

Os 12 testes conferem: Lamport, a corrida pelo mesmo assento, muitas compras
concorrentes sem venda dupla, réplicas iguais, eleição inicial, a falha e a
volta do líder, a abertura de vendas pelo líder, a lista de nós ativos, a
correção de dois líderes ao mesmo tempo, a réplica perdida que não pode
causar venda dupla e a regra de conflito pela ordem de Lamport.

## Limitações conhecidas (planejadas para a Entrega 2)

- Os dados ficam só na memória.
- Uma réplica perdida é corrigida na próxima vez que alguém pede aquele
  assento (a resposta da permissão traz a venda). Até lá, o mapa daquele nó
  fica desatualizado.
- O sistema considera "desligado" um nó sem conexão. Se um nó fica isolado
  de TODOS os outros, ele e o resto podem vender o mesmo assento. Quando as
  vendas se encontram, vale a primeira na ordem de Lamport, mas uma delas já
  foi confirmada ao comprador. Para impedir isso, a Entrega 2 pode exigir
  quórum (maioria dos nós) ou usar 2PC.
