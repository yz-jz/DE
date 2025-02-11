import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

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
        print("yield Success")
        yield page

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

load_info = pipeline.run(load_data, write_disposition="replace")
print(load_info)

df = pipeline.dataset(dataset_type='default').nyc_taxi.df()
print(df)

with pipeline.sql_client() as client:
    response = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM nyc_taxi;
            """
        )
    print(response)
