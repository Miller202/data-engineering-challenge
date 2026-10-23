-- Camada RAW: copia dos arquivos parquet.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.yellow_tripdata (
    vendorid              BIGINT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       DOUBLE PRECISION,
    trip_distance         DOUBLE PRECISION,
    ratecodeid            DOUBLE PRECISION,
    store_and_fwd_flag    TEXT,
    pulocationid          BIGINT,
    dolocationid          BIGINT,
    payment_type          BIGINT,
    fare_amount           DOUBLE PRECISION,
    extra                 DOUBLE PRECISION,
    mta_tax               DOUBLE PRECISION,
    tip_amount            DOUBLE PRECISION,
    tolls_amount          DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION,
    source_file           TEXT        NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY LIST (source_file);
