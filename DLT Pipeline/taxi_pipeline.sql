-- Bronze layer: Raw data ingestion
CREATE LIVE TABLE taxi_raw_records
AS
SELECT *
FROM READ_FILES(
  "dbfs:/FileStore/tables/train_1000.csv",
  format => "csv",
  header => "true"
)
WHERE trip_distance > 0;
-- Silver layer 1: Flagged rides
-- Suspicious = fare too high for the distance, or no passengers
CREATE LIVE TABLE flagged_rides
AS
SELECT
  id,
  pickup_datetime,
  fare_amount,
  trip_distance,
  passenger_count,
  pickup_longitude,
  pickup_latitude,
  date_trunc("week", pickup_datetime) AS week,
  CASE 
      WHEN fare_amount > trip_distance * 6 THEN 'High Fare'
      WHEN passenger_count = 0 THEN 'Invalid Passenger Count'
      ELSE 'Normal'
  END AS flag_reason
FROM LIVE.taxi_raw_records
WHERE fare_amount > trip_distance * 6
   OR passenger_count = 0;


-- Silver layer 2: Weekly statistics
CREATE LIVE TABLE weekly_stats
AS
SELECT
  date_trunc("week", pickup_datetime) AS week,
  ROUND(AVG(fare_amount),2) AS avg_fare,
  ROUND(AVG(trip_distance),2) AS avg_distance,
  SUM(passenger_count) AS passengers,
  COUNT(*) AS total_rides
FROM LIVE.taxi_raw_records
GROUP BY week
ORDER BY week;
-- Gold layer: Top N rides to investigate
CREATE LIVE TABLE top_3
AS
SELECT
  f.id,
  f.week,
  f.fare_amount,
  f.trip_distance,
  f.passenger_count,
  f.pickup_longitude,
  f.pickup_latitude,
  w.avg_fare,
  w.avg_distance,
  w.passengers,
  w.total_rides
FROM LIVE.flagged_rides f
LEFT JOIN LIVE.weekly_stats w
  ON f.week = w.week
ORDER BY f.fare_amount DESC
LIMIT 3;
