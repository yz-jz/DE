import duckdb

conn = duckdb.connect("./ny_taxi_pipeline.duckdb")

# Set search path to the dataset
conn.sql("SET search_path = 'ny_taxi_data'")

# Describe the dataset
print(conn.sql("DESCRIBE").df())
