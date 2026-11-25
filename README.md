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

## Entrega 2: tratamento de falhas e controle de réplicas

| O que | Onde está | O que resolve |
|---|---|---|
| **Controle de réplicas com quórum** | `src/no_ingressos.py` (`_vender_com_quorum`, `_reservar`) | A venda só é confirmada se a **maioria** dos nós (2 de 3) reservar o assento. Um nó isolado, ou o lado menor de uma rede dividida, não consegue vender. Duas maiorias sempre têm um nó em comum, e cada nó aceita só uma reserva por assento: não há venda dupla |
| Reserva com prazo | `src/no_ingressos.py` (`reserva_expira`) | Se o nó que reservou cair antes de confirmar, a reserva expira em 10 s |
| **Persistência em disco** | `src/persistencia.py` | Cada nó grava as vendas confirmadas em `dados/no_<id>.json` (escrita atômica). Se todos caírem, nada se perde |
| **Sincronização periódica** (anti-entropia) | `src/no_ingressos.py` (`_sincronizar_com`) | A cada ~6 s, o líder junta as vendas dos nós ativos e cada nó copia do líder o que falta. Corrige confirmações perdidas |
| Detecção de falha do líder e nova eleição | `src/no_ingressos.py`, `src/eleicao_bully.py` | Líder considerado morto após 2 heartbeats perdidos; o maior nó ativo assume |
| Recuperação de um nó que volta | `src/no_ingressos.py` (`_carregar_do_disco`, `_sincronizar_estado`) | Lê o próprio arquivo e junta o mapa de todos os nós ativos |
| Réplica perdida corrigida pela permissão | `src/exclusao_mutua.py` | Quem dá a permissão do Ricart-Agrawala conta se o assento já foi vendido |

Fluxo de uma compra (2 fases dentro da seção crítica do Ricart-Agrawala):

```
Comprar(A5) -> PedirAcesso a todos (Ricart-Agrawala) -> seção crítica
            -> Fase 1: ReservarAssento em todos  (cada nó aceita 1 reserva por assento)
            -> maioria aceitou?  sim -> Fase 2a: ConfirmarVenda em todos -> OK
                                 não -> Fase 2b: CancelarReserva         -> RECUSADO
```

## Estrutura

```
proto/ingressos.proto   contrato gRPC (mensagens e funções)
src/relogio_lamport.py  relógio de Lamport
src/exclusao_mutua.py   Ricart-Agrawala
src/eleicao_bully.py    Bully + papel do líder
src/no_ingressos.py     lógica do nó: compra, quórum, heartbeat, sincronização
src/persistencia.py     gravação das vendas em disco (dados/no_<id>.json)
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

Cada nó grava as vendas em `dados/no_<id>.json`. Para começar do zero, desligue
todos os nós e apague a pasta `dados`.

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

Entrega 2 (quórum e persistência):

11. Derrube os nós 2 e 3. `status` mostra **SEM QUÓRUM**, e
    `comprar 1 C1 Bia` é **recusado**: com 1 de 3 nós, não há maioria.
12. Religue o nó 2 e repita `comprar 1 C1 Bia`: **vendido** (2 de 3).
13. Derrube os 3 nós e religue só um: `mapa` mostra as vendas lidas do disco.

## Testes

```bash
python tests/test_algoritmos.py
```

Os 18 testes conferem: Lamport, a corrida pelo mesmo assento, muitas compras
concorrentes sem venda dupla, réplicas iguais, eleição inicial, a falha e a
volta do líder, a abertura de vendas pelo líder, a lista de nós ativos, a
correção de dois líderes ao mesmo tempo, a réplica perdida que não pode
causar venda dupla e a regra de conflito pela ordem de Lamport.

Entrega 2: nó isolado não vende; minoria sobrevivente não vende; partição
assimétrica (nós 1 e 2 sem comunicação, ambos falando com o 3) sem venda
dupla; reserva de um nó que caiu expira; persistência após a queda de todos
os nós; sincronização periódica corrige uma confirmação perdida.

## Limitações conhecidas

- Com 3 nós, o sistema tolera a falha de 1 nó. Com 2 nós fora, ele para de
  vender (por segurança): é a troca entre disponibilidade e consistência.
- A lista de nós é fixa (`--peers`): incluir um nó novo exige reiniciar os outros.
- Cada compra troca 6 × (N − 1) mensagens (pedido e resposta de permissão,
  de reserva e de confirmação): com muitos nós, as compras ficam mais lentas.
- As consultas (`mapa`) leem só a cópia do nó consultado, sem quórum: logo
  depois de uma falha, um nó pode mostrar um mapa ainda não sincronizado.
