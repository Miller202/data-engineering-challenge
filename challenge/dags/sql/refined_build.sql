-- Camada REFINED: tabela final, pronta para analise.
-- Acrescenta um identificador, as datas de embarque/desembarque, a duracao e a
-- velocidade media, alem de um indice na data de embarque (filtro mais comum).
-- A coluna source_file fica apenas na trusted.
CREATE SCHEMA IF NOT EXISTS refined;

DROP TABLE IF EXISTS refined.yellow_trips;

CREATE TABLE refined.yellow_trips (
    trip_id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    vendor_id             SMALLINT,
    pickup_datetime       TIMESTAMP NOT NULL,
    dropoff_datetime      TIMESTAMP NOT NULL,
    pickup_date           DATE      NOT NULL,
    dropoff_date          DATE      NOT NULL,
    trip_duration_minutes NUMERIC(8, 2),
    passenger_count       SMALLINT,
    trip_distance         NUMERIC(12, 2) NOT NULL,
    avg_speed_mph         NUMERIC(8, 2),
    ratecode_id           SMALLINT,
    store_and_fwd_flag    CHAR(1),
    pu_location_id        INTEGER,
    do_location_id        INTEGER,
    payment_type          SMALLINT,
    fare_amount           NUMERIC(12, 2),
    extra                 NUMERIC(12, 2),
    mta_tax               NUMERIC(12, 2),
    tip_amount            NUMERIC(12, 2),
    tolls_amount          NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    total_amount          NUMERIC(12, 2),
    congestion_surcharge  NUMERIC(12, 2),
    airport_fee           NUMERIC(12, 2)
);

INSERT INTO refined.yellow_trips (
    vendor_id, pickup_datetime, dropoff_datetime, pickup_date, dropoff_date,
    trip_duration_minutes, passenger_count, trip_distance, avg_speed_mph,
    ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
    total_amount, congestion_surcharge, airport_fee
)
SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_datetime::DATE,
    dropoff_datetime::DATE,
    ROUND(EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) / 60, 2),
    passenger_count,
    trip_distance,
    ROUND(trip_distance / (EXTRACT(EPOCH FROM dropoff_datetime - pickup_datetime) / 3600), 2),
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM trusted.yellow_tripdata;

CREATE INDEX idx_yellow_trips_pickup_date ON refined.yellow_trips (pickup_date);

ANALYZE refined.yellow_trips;
