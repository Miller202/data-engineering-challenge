# Solução: ETL de corridas de táxi de NY com Airflow e PostgreSQL

Pipeline de ETL orquestrado pelo **Apache Airflow** que carrega as corridas de táxi amarelo de Nova York de 2022 (12 arquivos parquet, ~39,7 milhões de registros) em um **PostgreSQL**, organizado nas camadas **raw**, **trusted** e **refined**, e responde às perguntas do desafio com SQL.

## Sumário

- [Arquitetura](#arquitetura)
- [Estrutura do diretório](#estrutura-do-diretório)
- [Como executar](#como-executar)
- [O pipeline](#o-pipeline)
- [Camadas de dados](#camadas-de-dados)
- [Decisões tomadas](#decisões-tomadas)
- [Dificuldades e soluções](#dificuldades-e-soluções)
- [Respostas](#respostas)
- [Possíveis melhorias](#possíveis-melhorias)

## Arquitetura

```
 nyc-tlc-data/*.parquet.gz          Docker Compose
 (montado como somente leitura)   ┌──────────────────────────────────────────────┐
            │                     │  Airflow (LocalExecutor)                     │
            └────────────────────►│  webserver :8080  ·  scheduler               │
                                  │        │                                     │
                                  │        │ DAG nyc_taxi_etl                    │
                                  │        ▼                                     │
                                  │  postgres-dw :5433 (banco nyc_taxi)          │
                                  │    raw ──► trusted ──► refined               │
                                  │                                              │
                                  │  postgres-airflow (metadados do Airflow)     │
                                  └──────────────────────────────────────────────┘
```

| Serviço             | Função                                                    |
|---------------------|-----------------------------------------------------------|
| `postgres-airflow`  | Banco de metadados do Airflow                             |
| `postgres-dw`       | Banco do desafio (`nyc_taxi`), exposto na porta **5433**  |
| `airflow-init`      | Migra o banco do Airflow e cria o usuário admin           |
| `airflow-webserver` | Interface web em **http://localhost:8080**                |
| `airflow-scheduler` | Agenda e executa as tasks (LocalExecutor)                 |

## Estrutura do diretório

```
challenge/
├── docker-compose.yaml        # Airflow + PostgreSQL
├── .env.example               # variáveis opcionais (usuário, senhas, portas)
├── dags/
│   ├── nyc_taxi_etl.py        # DAG do ETL
│   └── sql/
│       ├── raw_create.sql     # cria a tabela raw particionada
│       ├── trusted_build.sql  # tipagem e regras de qualidade
│       └── refined_build.sql  # tabela final
└── queries/                   # queries das perguntas do desafio
    ├── q1_total_registros.sql
    ├── q2_viagens_17_junho.sql
    ├── q3_dia_viagem_mais_longa.sql
    └── q4_estatisticas_distancia.sql
```

Os dados são lidos diretamente da pasta `nyc-tlc-data/`, na raiz do repositório.

## Como executar

**Pré-requisitos:** Docker com Docker Compose v2, pelo menos **4 GB de RAM** para o Docker (recomendado 8 GB) e **25 GB de disco livre** (o banco final ocupa ~21 GB).

```bash
cd challenge

cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env

docker compose up -d
```

1. Acesse **http://localhost:8080** (usuário `admin`, senha `admin`).
2. Ative e execute a DAG **`nyc_taxi_etl`** (botão ▶ *Trigger DAG*). Pelo terminal:
   ```bash
   docker compose exec airflow-scheduler airflow dags unpause nyc_taxi_etl
   docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_etl
   ```
3. Ao final, as respostas aparecem no log da task `answer_questions`.

Para consultar o banco diretamente:

```bash
docker compose exec postgres-dw psql -U nyc -d nyc_taxi
```

Para derrubar tudo e apagar os dados: `docker compose down -v`.

## O pipeline

```
create_raw_table ──┐
                   ├──► load_raw [x12] ──► build_trusted ──► check_trusted ──► build_refined ──► answer_questions
list_source_files ─┘
```

| Task                | O que faz                                                                                   |
|---------------------|---------------------------------------------------------------------------------------------|
| `create_raw_table`  | Cria o schema `raw` e a tabela `raw.yellow_tripdata`, particionada por arquivo de origem    |
| `list_source_files` | Lista os arquivos `yellow_tripdata_2022-*.parquet*` disponíveis                              |
| `load_raw`          | *Dynamic task mapping*: uma task por arquivo, carregando a sua partição via `COPY`           |
| `build_trusted`     | Cria `trusted.yellow_tripdata` com tipagem, nomes padronizados e regras de qualidade        |
| `check_trusted`     | Compara os volumes raw x trusted e falha se a trusted estiver vazia                         |
| `build_refined`     | Cria a tabela final `refined.yellow_trips` com colunas derivadas e índice                   |
| `answer_questions`  | Executa as queries de `queries/` e registra as respostas no log                             |

## Camadas de dados

### raw: `raw.yellow_tripdata`

Cópia dos arquivos, com as mesmas colunas e tipos do parquet, mais `source_file` (arquivo de origem) e `loaded_at` (momento da carga). A tabela é particionada por `LIST (source_file)`, com uma partição por mês (`raw.yellow_tripdata_2022_01`, ...). **39.656.098 registros.**

### trusted: `trusted.yellow_tripdata`

Tipos adequados (`SMALLINT`, `INTEGER`, `NUMERIC(12,2)` para valores monetários e distância), colunas em *snake_case* (`vendor_id`, `pu_location_id`, ...) e as regras de qualidade abaixo. **38.800.155 registros.**

| # | Regra                                                         | Registros removidos |
|---|---------------------------------------------------------------|--------------------:|
| 1 | Embarque fora de 2022 (há datas de 2001 a 2023)               | 561                 |
| 2 | Desembarque antes ou igual ao embarque, ou duração > 24 horas | 32.242              |
| 3 | Distância menor ou igual a zero                               | 557.352             |
| 4 | Velocidade média acima de 100 mph (distâncias impossíveis)    | 46.909              |
| 5 | Tarifa ou valor total negativos (estornos/cancelamentos)      | 218.879             |
| 6 | Registros duplicados                                          | 0                   |
|   | **Total**                                                     | **855.943 (2,16%)** |

As contagens são sequenciais: cada linha conta os registros removidos pela regra que ainda não tinham sido removidos pelas anteriores.

### refined: `refined.yellow_trips`

Tabela final para análise: os dados da trusted mais `trip_id` (chave primária), `pickup_date`, `dropoff_date`, `trip_duration_minutes` e `avg_speed_mph`, com índice em `pickup_date`. **38.800.155 registros.**

## Decisões tomadas

- **Dois PostgreSQL.** O banco de metadados do Airflow fica separado do banco do desafio, como seria em produção. Assim os dados podem ser consultados ou recriados sem afetar o Airflow.
- **Carga com `COPY` em lotes.** Com ~40 milhões de linhas, `INSERT` ou `pandas.to_sql` seriam lentos demais. Cada arquivo é lido com `pyarrow` em lotes de 250 mil linhas, convertido para CSV em memória e enviado com `COPY FROM STDIN`. No fim, o total carregado é conferido com o número de linhas do arquivo.
- **Idempotência.** A DAG pode ser reexecutada sem duplicar dados: cada arquivo tem a sua partição, que é truncada antes da carga, e as camadas trusted e refined são recriadas por completo, dentro de uma transação.
- **Paralelismo controlado.** No máximo 2 arquivos são carregados ao mesmo tempo (`max_active_tis_per_dag=2`), para limitar o uso de memória.
- **Transformações em SQL.** Trusted e refined são construídas dentro do próprio PostgreSQL com `CREATE TABLE AS` / `INSERT ... SELECT`, sem trafegar dados pelo Airflow. Os scripts ficam em arquivos `.sql` versionados.
- **Interpretação das perguntas.**
  - *Pergunta 2:* "iniciadas e finalizadas no dia 17 de junho" = embarque **e** desembarque em 17/06/2022.
  - *Pergunta 3:* "viagem mais longa percorrida" = maior `trip_distance` (milhas). O "dia" é a data de embarque.
  - *Pergunta 4:* desvio padrão amostral (`STDDEV_SAMP`) e quartis por interpolação (`PERCENTILE_CONT`).

## Dificuldades e soluções

### 1. Disco esgotado ao criar a camada refined

Na primeira execução completa, a task `build_refined` falhou com `No space left on device`. As três camadas somam ~40 milhões de linhas cada, e o PostgreSQL ainda mantinha ~4 GB de WAL (o log de transações) por causa das cargas grandes. Para resolver:

- `wal_level=minimal` e `max_wal_senders=0`: como não há replicação neste ambiente, `COPY` e `CREATE TABLE AS` em tabelas criadas ou truncadas na mesma transação deixam de gerar WAL. A carga fica mais rápida e o diretório de WAL caiu de **4,1 GB para ~100 MB**.
- `max_wal_size` reduzido de 4 GB para 1 GB.
- A coluna `source_file` (linhagem) ficou só na raw e na trusted, e a refined manteve apenas o índice realmente usado (`pickup_date`), economizando alguns GB.

### 2. Outliers que tornavam a pergunta 3 sem sentido

Nos dados brutos, a "viagem mais longa" teria **389.678 milhas percorridas em 13 minutos**, e há mais de mil registros acima de 1.000 milhas. Um corte fixo de distância seria arbitrário e poderia descartar viagens longas reais. A solução foi uma regra de **velocidade média máxima de 100 mph**, que remove distâncias fisicamente impossíveis para a duração registrada e preserva viagens longas plausíveis, como a resposta final (380 milhas em ~6 h 45 min, ~56 mph).

### 3. Volume de dados (~40 milhões de linhas)

A abordagem mais comum, ler com pandas e gravar com `to_sql`, seria lenta demais e consumiria muita memória. Cada arquivo tem um único *row group* (até 3,6 milhões de linhas), então a leitura foi feita com `pyarrow` em lotes de 250 mil linhas, convertidos para CSV em memória e enviados com `COPY FROM STDIN`. Para não estourar a memória do Docker, no máximo 2 arquivos são carregados em paralelo. Com isso, a carga raw completa leva ~4 minutos.

### 4. Arquivos `.parquet.gz` que não são gzip

Apesar da extensão, os arquivos não estão compactados com gzip: são parquet comum, que já é comprimido internamente (verificado pelo cabeçalho `PAR1`). Por isso são lidos diretamente, sem etapa de descompressão.

## Respostas

Todas as queries consultam a tabela final `refined.yellow_trips` e estão em [`queries/`](queries/).

### 1. Qual o total de registros na tabela final?

```sql
SELECT COUNT(*) AS total_registros
FROM refined.yellow_trips;
```

**Resposta: 38.800.155 registros** (de 39.656.098 na camada raw).

### 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?

```sql
SELECT COUNT(*) AS total_viagens
FROM refined.yellow_trips
WHERE pickup_date  = DATE '2022-06-17'
  AND dropoff_date = DATE '2022-06-17';
```

**Resposta: 120.837 viagens.**

### 3. Qual foi o dia da viagem mais longa percorrida?

```sql
SELECT
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    trip_duration_minutes
FROM refined.yellow_trips
ORDER BY trip_distance DESC
LIMIT 1;
```

**Resposta: 13/06/2022.** A viagem percorreu 380,13 milhas, de 01:36:18 às 08:21:44 (cerca de 6 h 45 min).

### 4. Qual a média, o desvio padrão, o mínimo, o máximo e os quartis da distribuição de distância percorrida nas viagens totais?

```sql
SELECT
    ROUND(AVG(trip_distance), 4)                                AS media,
    ROUND(STDDEV_SAMP(trip_distance), 4)                        AS desvio_padrao,
    MIN(trip_distance)                                          AS minimo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance) AS q2_mediana,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3_75,
    MAX(trip_distance)                                          AS maximo
FROM refined.yellow_trips;
```

**Resposta (em milhas):**

| Média  | Desvio padrão | Mínimo | Q1 (25%) | Mediana (50%) | Q3 (75%) | Máximo |
|-------:|--------------:|-------:|---------:|--------------:|---------:|-------:|
| 3,5560 | 4,4886        | 0,01   | 1,14     | 1,90          | 3,60     | 380,13 |

## Possíveis melhorias

- Particionar a refined por mês de embarque, para acelerar consultas por período.
- Registrar os descartes da trusted em uma tabela de auditoria (por regra e por execução) em vez de só no log.
- Testes de qualidade automatizados (por exemplo, Great Expectations ou dbt tests) e testes unitários da DAG.
- Agendar a DAG por mês, carregando só o arquivo novo de cada período.
