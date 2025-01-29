# Module 2 HW

## Assignment:

Assignment code is in kestra.yaml

## Quiz Questions:
### Rows in Yellow Taxi data for the year 2020:

```sql

SELECT FORMAT_DATE('%Y',DATE(tpep_dropoff_datetime)) AS grouped_date,
    COUNT(*) FROM `ProjectId.module2.yellow_tripdata` 
    GROUP BY FORMAT_DATE('%Y',DATE(tpep_dropoff_datetime)) 
    HAVING grouped_date = '2020'

```
### Rows in Green data for the year 2020:

```sql

SELECT FORMAT_DATE('%Y',DATE(tpep_dropoff_datetime)) AS grouped_date,
    COUNT(*) FROM `ProjectId.module2.green_tripdata` 
    GROUP BY FORMAT_DATE('%Y',DATE(tpep_dropoff_datetime)) 
    HAVING grouped_date = '2020'

```
### Rows in Yellow Taxi data for March 2021:

```sql

SELECT FORMAT_DATE('%Y-%m',DATE(tpep_dropoff_datetime)) AS grouped_date,
    COUNT(*) FROM `ProjectId.module2.yellow_tripdata` 
    GROUP BY FORMAT_DATE('%Y-%m',DATE(tpep_dropoff_datetime)) 
    HAVING grouped_date = '2021-03'

```
