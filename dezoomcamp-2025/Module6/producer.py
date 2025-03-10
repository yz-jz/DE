import json
import polars as pl
from time import time 

from kafka import KafkaProducer

data = pl.read_csv("green_tripdata_2019-10.csv")
data = data.select([
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
])

messages = data.to_dicts()

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

start = time()

for message in messages:
    producer.send("green-trips",value=message)
producer.flush()

end = time()

print(end-start)
