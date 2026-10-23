-- Camada TRUSTED: dados tipados, com nomes padronizados e regras de qualidade.
--
-- Regras aplicadas:
--   1. Embarque dentro de 2022.
--   2. Desembarque depois do embarque e duracao de no maximo 24 horas.
--   3. Distancia maior que zero.
--   4. Velocidade media de no maximo 100 mph (remove distancias impossiveis,
--      como 389 mil milhas percorridas em 13 minutos).
--   5. Valores de tarifa e total nao negativos (estornos e cancelamentos).
--   6. Remocao de registros duplicados.
CREATE SCHEMA IF NOT EXISTS trusted;

DROP TABLE IF EXISTS trusted.yellow_tripdata;

CREATE TABLE trusted.yellow_tripdata AS
SELECT DISTINCT
    vendorid::SMALLINT                     AS vendor_id,
    tpep_pickup_datetime                   AS pickup_datetime,
    tpep_dropoff_datetime                  AS dropoff_datetime,
    passenger_count::SMALLINT              AS passenger_count,
    trip_distance::NUMERIC(12, 2)          AS trip_distance,
    ratecodeid::SMALLINT                   AS ratecode_id,
    store_and_fwd_flag::CHAR(1)            AS store_and_fwd_flag,
    pulocationid::INTEGER                  AS pu_location_id,
    dolocationid::INTEGER                  AS do_location_id,
    payment_type::SMALLINT                 AS payment_type,
    fare_amount::NUMERIC(12, 2)            AS fare_amount,
    extra::NUMERIC(12, 2)                  AS extra,
    mta_tax::NUMERIC(12, 2)                AS mta_tax,
    tip_amount::NUMERIC(12, 2)             AS tip_amount,
    tolls_amount::NUMERIC(12, 2)           AS tolls_amount,
    improvement_surcharge::NUMERIC(12, 2)  AS improvement_surcharge,
    total_amount::NUMERIC(12, 2)           AS total_amount,
    congestion_surcharge::NUMERIC(12, 2)   AS congestion_surcharge,
    airport_fee::NUMERIC(12, 2)            AS airport_fee,
    source_file
FROM raw.yellow_tripdata
WHERE tpep_pickup_datetime >= TIMESTAMP '2022-01-01'
  AND tpep_pickup_datetime <  TIMESTAMP '2023-01-01'
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND tpep_dropoff_datetime - tpep_pickup_datetime <= INTERVAL '24 hours'
  AND trip_distance > 0
  AND trip_distance / (EXTRACT(EPOCH FROM tpep_dropoff_datetime - tpep_pickup_datetime) / 3600) <= 100
  AND fare_amount >= 0
  AND total_amount >= 0;
