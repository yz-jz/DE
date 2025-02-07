#Module 3 HW

##Question1:
###Count of records Yellow Taxi Data 2024:

```sql
SELECT COUNT(1) FROM `projectID.module3.yellow_tripdata`
```

##Question2:
###Distinct number of PULocationIDs for the entire dataset
1. External Table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `projectID.module3.yellow_tripdata_external`
```

2. Materialized Table:
```sql
SELECT COUNT(DISTINCT PULocationID) FROM `projectID.module3.yellow_tripdata`
```

##Question3:
###Retrieve PULocationID from materialized table
1. PULocationID
```sql
SELECT PULocationID FROM `projectID.module3.yellow_tripdata`;
```

2. PULocationID and DOLocationID
```sql
SELECT PULocationID, DOLocationID FROM `projectID.module3.yellow_tripdata`;
```
##Question4:
###Number of records with fare_amount of 0:
```sql
SELECT COUNT(1) FROM `projectID.module3.yellow_tripdata` WHERE fare_amount = 0
```

##Question5:
###Best strategy to make an optimized table in Big Query when query will always filter based on tpep_dropoff_datetime and order the results by VendorID
#### Table creation:
```sql
CREATE TABLE IF NOT EXISTS `projectID.module3.yellow_tripdata_optimized` (
  VendorID INTEGER, 
  tpep_pickup_datetime TIMESTAMP,	
  tpep_dropoff_datetime TIMESTAMP,	
  passenger_count INTEGER	,
  trip_distance FLOAT64	,
  RatecodeID INTEGER	,	
  store_and_fwd_flag STRING	,
  PULocationID INTEGER ,
  DOLocationID INTEGER ,
  payment_type INTEGER ,
  fare_amount FLOAT64 ,
  extra FLOAT64	,	
  mta_tax FLOAT64	,
  tip_amount FLOAT64	,
  tolls_amount FLOAT64	,
  improvement_surcharge FLOAT64	,
  total_amount FLOAT64	,
  congestion_surcharge FLOAT64	,
  Airport_fee FLOAT64
) PARTITION BY 
    DATE(tpep_dropoff_datetime)
  CLUSTER BY
    VendorID;

INSERT INTO `projectID.module3.yellow_tripdata_optimized` (
  SELECT * FROM `projectID.module3.yellow_tripdata`
)
```

##Question6:
###Query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
1. Non-partitioned

```sql
SELECT DISTINCT(VendorID) FROM `projectID.module3.yellow_tripdata` WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-31'
```

2. Partitioned

```sql
SELECT DISTINCT(VendorID) FROM `projectID.module3.yellow_tripdata_optimized` WHERE tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-31'
```
