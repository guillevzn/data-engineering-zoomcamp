
# Question 1
```bash
docker run -it --entrypoint bash python:3.12.8
pip --version
```

- Result: pip 24.3.1

# Question 2

- Response: postgres:5432


# Question 3

```SQL
SELECT
    COUNT(CASE WHEN trip_distance <= 1 THEN 1 END) AS "≤1",
    COUNT(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 END) AS "1-3",
    COUNT(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 END) AS "3-7",
    COUNT(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 END) AS "7-10",
    COUNT(CASE WHEN trip_distance > 10 THEN 1 END) AS ">10"
FROM green_trips
WHERE lpep_pickup_datetime >= '2019-10-01' AND lpep_pickup_datetime < '2019-11-01';
```

- Response:  104,838; 199,013; 109,645; 27,688; 35,202


# Question 4

```SQL
SELECT DATE(lpep_pickup_datetime) AS pickup_day
FROM green_trips
ORDER BY trip_distance DESC
LIMIT 1;
```

- Response: 2019-10-31


# Question 5

```SQL
SELECT z."zone", SUM(t.total_amount) AS total
FROM green_trips t
JOIN taxi_zones z ON t."pulocationid" = z."locationid"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."zone"
HAVING SUM(t.total_amount) > 13000
ORDER BY total DESC;
```

- Response: East Harlem North, East Harlem South, Morningside Heights


# Question 6
```SQL
SELECT z."zone" AS dropoff_zone
FROM green_trips t
JOIN taxi_zones p ON t."pulocationid" = p."locationid"
JOIN taxi_zones z ON t."dolocationid" = z."locationid"
WHERE p."zone" = 'East Harlem North'
  AND DATE(lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
ORDER BY t.tip_amount DESC
LIMIT 1;
```

- Response: JFK Airport