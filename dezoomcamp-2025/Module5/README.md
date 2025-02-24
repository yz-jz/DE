# Module 4 HW

## Question 1:
### Spark version

``` python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
.master("local[*]")\
.appName("module5")\
.getOrCreate()

spark.version
```


## Question 2:
### Repartition

``` python

df = spark.read.parquet("./yellow_tripdata_2024-10.parquet")

df = df.repartition(4)

df.write.mode("append").parquet("partitions/")

```
## Question 3:
### Records count

``` python
df.filter(df.tpep_pickup_datetime >= "2024-10-15 00:00:00")\
.filter(df.tpep_pickup_datetime < "2024-10-16 00:00:00")\
.count()
```

## Question 4:
### Longest trip
``` python
from pyspark.sql.functions import col, max
df = df.withColumn("trip_duration",(df.tpep_dropoff_datetime.cast("long") - df.tpep_pickup_datetime.cast("long"))/3600)
df.select(max(col("trip_duration"))).show()
```

## Question 6:
### Least frequent pickup location zone

``` python

from pyspark.sql.functions import broadcast
zones = spark.read.csv("taxi_zone_lookup.csv",header=True)

df.join(
        broadcast(zones),
        zones.LocationID == df.PULocationID
).groupBy("Zone")\
.count()\
.orderBy("count")\
.head(1)

```
