"""
ETL das corridas de taxi amarelo de Nova York (2022) para o PostgreSQL.

Fluxo:
    create_raw_table -> list_source_files -> load_raw (1 task por arquivo)
    -> build_trusted -> check_trusted -> build_refined

Camadas (schemas no banco nyc_taxi):
    raw      copia dos arquivos parquet, particionada por arquivo de origem
    trusted  dados tipados, padronizados e filtrados por regras de qualidade
    refined  tabela final para analise, com colunas derivadas e indices
"""

from __future__ import annotations

import io
import logging
import os
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "nyc_taxi_dw"
DATA_DIR = Path(os.getenv("NYC_TLC_DATA_DIR", "/opt/airflow/data"))
FILE_PATTERN = "yellow_tripdata_2022-*.parquet*"
BATCH_SIZE = 250_000

# Colunas na ordem da tabela raw.yellow_tripdata.
RAW_COLUMNS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

log = logging.getLogger(__name__)


def partition_name(file_name: str) -> str:
    """yellow_tripdata_2022-01.parquet.gz -> yellow_tripdata_2022_01"""
    return file_name.split(".")[0].replace("-", "_")


@dag(
    dag_id="nyc_taxi_etl",
    description="ETL das corridas de taxi de NY (2022): raw -> trusted -> refined",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    default_args={"retries": 1},
    tags=["nyc-taxi", "etl"],
    doc_md=__doc__,
)
def nyc_taxi_etl():
    create_raw_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/raw_create.sql",
    )

    @task
    def list_source_files() -> list[str]:
        files = sorted(str(path) for path in DATA_DIR.glob(FILE_PATTERN))
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo {FILE_PATTERN} encontrado em {DATA_DIR}")
        log.info("%d arquivos encontrados: %s", len(files), files)
        return files

    @task(max_active_tis_per_dag=2)
    def load_raw(file_path: str) -> int:
        """Carrega um arquivo parquet na sua particao da camada raw, via COPY."""
        file_name = Path(file_path).name
        partition = partition_name(file_name)
        parquet = pq.ParquetFile(file_path)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        total = 0
        try:
            with conn.cursor() as cur:
                # Idempotencia: recria a particao do arquivo vazia antes da carga.
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS raw.{partition}
                        PARTITION OF raw.yellow_tripdata FOR VALUES IN (%s);
                    TRUNCATE raw.{partition};
                    """,
                    (file_name,),
                )
                copy_sql = (
                    f"COPY raw.{partition} ({', '.join(RAW_COLUMNS)}, source_file) "
                    "FROM STDIN WITH (FORMAT csv)"
                )
                for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
                    batch = batch.rename_columns([name.lower() for name in batch.schema.names])
                    table = pa.Table.from_batches([batch]).select(RAW_COLUMNS)
                    table = table.append_column(
                        "source_file", pa.array([file_name] * table.num_rows, pa.string())
                    )
                    buffer = io.BytesIO()
                    pacsv.write_csv(table, buffer, pacsv.WriteOptions(include_header=False))
                    buffer.seek(0)
                    cur.copy_expert(copy_sql, buffer)
                    total += table.num_rows
            conn.commit()
        finally:
            conn.close()

        expected = parquet.metadata.num_rows
        if total != expected:
            raise ValueError(f"{file_name}: {total} linhas carregadas, esperado {expected}")
        log.info("%s: %d linhas carregadas em raw.%s", file_name, total, partition)
        return total

    build_trusted = SQLExecuteQueryOperator(
        task_id="build_trusted",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/trusted_build.sql",
    )

    @task
    def check_trusted() -> None:
        """Compara os volumes das camadas e falha se a trusted estiver vazia."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        raw_count = hook.get_first("SELECT COUNT(*) FROM raw.yellow_tripdata")[0]
        trusted_count = hook.get_first("SELECT COUNT(*) FROM trusted.yellow_tripdata")[0]
        if trusted_count == 0:
            raise ValueError("A camada trusted está vazia")
        removed = raw_count - trusted_count
        log.info(
            "raw: %d | trusted: %d | removidos pelas regras de qualidade: %d (%.2f%%)",
            raw_count,
            trusted_count,
            removed,
            100 * removed / raw_count,
        )

    build_refined = SQLExecuteQueryOperator(
        task_id="build_refined",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/refined_build.sql",
    )

    loaded = load_raw.expand(file_path=list_source_files())
    create_raw_table >> loaded
    loaded >> build_trusted >> check_trusted() >> build_refined


nyc_taxi_etl()
