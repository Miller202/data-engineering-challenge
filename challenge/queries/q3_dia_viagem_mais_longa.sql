-- 3. Qual foi o dia da viagem mais longa percorrida?
-- "Mais longa" = maior distancia percorrida (trip_distance, em milhas).
SELECT
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    trip_duration_minutes
FROM refined.yellow_trips
ORDER BY trip_distance DESC
LIMIT 1;
