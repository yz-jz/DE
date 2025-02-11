# Workshop/dlt HW

## Quiz Questions:
### dlt version:
``` bash
python -m dlt --version
```

### Define & Run the Pipeline (NYC Taxi API):
``` python
@dlt.resource(name="nyc_taxi")
def load_data(): 
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator= PageNumberPaginator(
            base_page = 1,
            total_path=None
        )
    )
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page
```

### Explore the loaded data:

``` python 
df = pipeline.dataset(dataset_type='default').nyc_taxi.df()
print(df)
```

### Trip Duration Analysis:
``` sql
SELECT
AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
FROM nyc_taxi;
```
