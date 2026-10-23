-- 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?
-- Considera as viagens com embarque E desembarque em 17/06/2022.
SELECT COUNT(*) AS total_viagens
FROM refined.yellow_trips
WHERE pickup_date  = DATE '2022-06-17'
  AND dropoff_date = DATE '2022-06-17';
