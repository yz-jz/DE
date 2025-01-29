### Trip segmentation count :
1. Query 1:

```sql
SELECT COUNT(*) AS number_of_trips FROM yellow_taxi_dataset 
WHERE lpep_dropoff_datetime >= DATE('2019-10-01') AND 
lpep_dropoff_datetime < DATE('2019-11-01')
AND trip_distance <= 1;

SELECT COUNT(*) AS number_of_trips FROM yellow_taxi_dataset 
WHERE lpep_dropoff_datetime >= DATE('2019-10-01') and lpep_dropoff_datetime < DATE('2019-11-01') AND trip_distance > 1 AND trip_distance
  <= 3;
  
SELECT COUNT(*) AS number_of_trips FROM yellow_taxi_dataset 
WHERE lpep_dropoff_datetime >= DATE('2019-10-01') AND lpep_dropoff_datetime < DATE('2019-11-01') AND trip_distance > 3 AND trip_distance
  <= 7;

SELECT COUNT(*) AS number_of_trips FROM yellow_taxi_dataset 
WHERE lpep_dropoff_datetime >= DATE('2019-10-01') and lpep_dropoff_datetime < DATE('2019-11-01') AND trip_distance > 7 AND trip_distance
  <= 10;

SELECT COUNT(*) AS number_of_trips FROM yellow_taxi_dataset 
WHERE lpep_dropoff_datetime >= DATE('2019-10-01') and lpep_dropoff_datetime < DATE('2019-11-01') AND trip_distance > 10;
```

### Longest trip for each day : 
2. Query 2:

```sql
WITH distance AS (
    SELECT DATE(lpep_pickup_datetime), MAX(trip_distance) AS max_distance 
    FROM yellow 
    GROUP BY DATE(lpep_pickup_datetime)
    ) SELECT * FROM distance ORDER BY max_distance DESC;
```

### Three biggest pickup zones : 
3. Query 3:

```sql
WITH summed AS (
    SELECT PULocationID, SUM(total_amount) AS total_amount_zone 
    FROM yellow_taxi_dataset
    WHERE DATE(lpep_pickup_datetime) = '2019-10-18' 
    GROUP BY "PULocationID"
    ) SELECT * FROM summed s 
    JOIN zones z ON s.PULocationID = z.LocationID 
    ORDER BY total_amount_zone DESC 
    LIMIT 3;
```

### Largest tip : 
4. Query 4:
```sql
WITH joined AS (
SELECT * FROM yellow_taxi_dataset y 
JOIN zones z ON y.PULocationID = z."LocationID" 
WHERE z.Zone = 'East Harlem North' 
AND DATE(y.lpep_dropoff_datetime) 
BETWEEN '2019-10-01' AND '2019-10-31'
), largest_tip AS (
SELECT *  FROM joined 
ORDER BY tip_amount DESC 
LIMIT 1
) SELECT z.Zone FROM largest_tip l 
JOIN zones z 
ON z.LocationID = l.DOLocationID;
 ```
