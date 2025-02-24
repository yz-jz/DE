import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
.master("local[*]")\
.appName("module5")\
.getOrCreate()

df = spark.read.parquet("./yellow_tripdata_2024-10.parquet")

df = df.repartition(4)

df.write.mode("append").parquet("partitions/")

df.filter(df.tpep_pickup_datetime >= "2024-10-15 00:00:00")\
.filter(df.tpep_pickup_datetime < "2024-10-16 00:00:00")\
.count()

from pyspark.sql.functions import col, max
df = df.withColumn("trip_duration",(df.tpep_dropoff_datetime.cast("long") - df.tpep_pickup_datetime.cast("long"))/3600)
df.select(max(col("trip_duration"))).show()


# +
from pyspark.sql.functions import broadcast
zones = spark.read.csv("taxi_zone_lookup.csv",header=True)

df.join(
        broadcast(zones),
        zones.LocationID == df.PULocationID
).groupBy("Zone")\
.count()\
.orderBy("count")\
.head(1)
