![Python 3.6.9](https://img.shields.io/badge/Python-3.6.9-light)
![Airflow 1.10.10](https://img.shields.io/badge/Airflow-1.10.10-red)
![GeoPandas 0.7](https://img.shields.io/badge/GeoPandas-0.7-purple)
![Spark 2.4.5](https://img.shields.io/badge/Spark-2.4.5-orange)
![PostGIS 2.4](https://img.shields.io/badge/PostGIS-2.4-darkblue)
![Dash 1.12](https://img.shields.io/badge/Dash-1.12-blue)
![MIT License](https://img.shields.io/badge/License-MIT-lightgrey)
# Where Cycle

*Getting New Yorkers Back to Business, Safely*

## Contents
1. [Purpose](README.md#purpose)
1. [Pipeline](README.md#pipeline)
1. [Summary](README.md#summary)
    - [Data](README.md#data)
    - [Preparation](README.md#preparation)
    - [Spark Reduction](README.md#spark-reduction)
    - [postGIS Tables](README.md#postgis-tables)
1. [Spark Optimization](README.md#spark-optimization)
1. [Setup](README.md#setup)
1. [Directory Structure](README.md#directory-structure)
1. [DAG Hierarchy](README.md#dag-hierarchy)
1. [License](README.md#license)

## Purpose
As health officials advised social distancing and businesses closed earlier this year, subway and bus ridership plummeted in many large cities. New York saw an almost 90% reduction by late April. Now, as the city is tentatively opening back up, people may be looking to return to their places of work and to support their favorite businesses, but they might be hesitant to utilize public transit, instead seeking open-air alternatives.

A cursory glance at some transit coverage in NYC makes it clear that, while Citibike is an awesome open-air solution, the available stations can’t immediately meet the needs of the outer boroughs: some expansion is required. **The goal of this pipeline is to determine which NYC taxi zones may be the best candidates for Citibike expansion by aggregating historical taxi & for-hire vehicle trips, Citibike trips & station density, and Yelp business statistics.**

*This project was developed by Josh Lang as part of his data engineering fellowship with Insight Data Science in the summer of 2020.*

## Pipeline
![Pipeline](https://github.com/josh-lang/where-cycle/blob/master/pipeline.png) <br/>
![DAG](https://github.com/josh-lang/where-cycle/blob/master/dag.png)

## Summary
If you'd prefer to jump right in and start clicking into the functions from that DAG above, then the file that produced it is [here](https://github.com/josh-lang/where-cycle/blob/master/src/airflow/where_cycle_dag.py). Since you can't navigate directly to everything from there, you may also find a glance at the [directory structure](README.md#directory-structure) below handy.

### Data
 - Citibike Trip Histories: [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/tripdata), [documentation](https://www.citibikenyc.com/system-data)
 - NYC Taxi & Limousine Commission Trip Records: [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/nyc-tlc), [documentation](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
 - Yelp Business Search API: [documentation](https://www.yelp.com/developers/documentation/v3/business_search)

### Preparation
 - In order to index everything by taxi zone, NYC-TLC's shapefile needs to be pulled down from S3, processed, and saved to PostgreSQL
     - Coordinate reference system is converted from NAD83 to WGS84
     - Each polygon is replaced with its equivalent multipolygon
     - All geometries are converted to well-known text
 - Centroids are then calculated for each taxi zone and used to query Yelp's API, requesting the 50 nearest businesses. These are cleaned and written as well
     - Invalid results and duplicates are removed
     - Coordinates are unnested and combined into point geometries
     - Like with taxi zones, geometries are converted to well-known text
 - Spark can't ingest zip files natively since Hadoop, which provides its underlying filesystem interface, does not support that compression codec. So, Citibike's zipped files need to be pulled out of S3, unzipped, and sent back to another S3 bucket before batch processing
     - Python's `io.BytesIO` class reads S3's *bytes-like objects* and makes this a quick streaming process

### Spark Reduction
 - Spark can read csv files directly via the s3a connector for Hadoop, and multiple URIs can be specified with globbing
     - Citibike's trip data is consistent, so parsing all of it requires only one path and one schema definition
     - TLC data is messier with 15 distinct csv headers over the corpus, but because this project isn't concerned with any columns that appear after trip dates and endpoint locations, 10 truncated schemas are sufficient for pulling everything in correctly
     - TLC trips before 2016-07 use coordinates for pickup and dropoff locations, while trips after 2016-06 use taxi zone IDs
     - Relevant columns are selected from csvs, and then they're all unioned together into 4 cached tables: Citibike trips, past TLC trips, modern TLC trips, and a small table for just the earliest for-hire vehicle trips
 - To aggregate visits by taxi zone, trip beginnings and endings need to be combined into endpoints and grouped by location. 4 tables are created in PostgreSQL:
     - Coordinates for unique Citibike stations within the taxi zone map's extent are pulled out separately from visit aggregation
     - Citibike visits are then aggragated by station ID
     - Past TLC visits are aggregated by coordinates within taxi zone extent rounded to 3 decimal places — neighborhood resolution
     - Modern TLC visits and those early for-hire vehicle visits are aggregated simply by taxi zone ID

### PostGIS Tables
 - All tables so far have been written to the *staging* schema in PostgreSQL. Now, that everything's there, some final processing with the PostGIS extension can be done
 - *geo_joined* schema
     - Citibike station coordinates are matched to taxi zone polygons to create a join table for Citibike visits
     - Past TLC visits are aggregated by the taxi zone their coordinates are within
 - *statistics* schema
     - Citibike stations and trips are aggregated by taxi zone using join table
     - Past TLC visits are unioned and summed with modern TLC visits using taxi zone IDs
     - Yelp business ratings and reviews are aggregated by the taxi zone their coordinates are within
 - *production* schema
     - Taxi zone geometries are converted to GeoJSON for Dash to plot on a choropleth map
     - Citibike, TLC, and Yelp statistics are joined to taxi zone dimensions for Dash to define toggleable scales for the choropleth map

## Spark Optimization
I tested a handful of methods and configuration changes trying to make the Spark piece of the pipeline run more efficiently. First, since I had already defined each TLC schema while taking my initial stab at ingestion, I wanted to see whether those explicit definitions were, in fact, significantly faster than just using Spark's `inferSchema` option. Defining schemas before reading files was faster (as expected), but it only reduced total runtime by **~2.1%**.

The most dramatic improvement came with caching each table of source CSVs before running the Spark SQL queries that transform them. This increased my total runtime savings to **~32.9%**!

After that, I found that lowering the number of shuffle partitions so that it matched the number of cores in my small cluster and doubling the maximum bytes in cached storage batches and in each partition could make things even faster, but only by so much. Changing these settings in my `spark-defaults.conf` file brought total runtime reduction to **~36.6%**:
| Property | Setting |
| -------- | ------- |
| spark.sql.files.maxPartitionBytes | 268435456 |
| spark.sql.inMemoryColumnarStorage.batchSize | 20000 |
| spark.sql.inMemoryColumnarStorage.compressed | true |
| spark.sql.shuffle.partitions | 12 |

## Setup
Python dependencies can be installed with the following command:
```sh
pip install -r requirements.txt
```

This project was built using an Apache Spark 2.4.5 / Hadoop 2.7 binary downloaded from [spark.apache.org](https://spark.apache.org/downloads.html). It reads from AWS S3 and writes to PostgreSQL, so a driver from [jdbc.postgresql.org](https://jdbc.postgresql.org) should be placed in `spark/jars/` and some configuration should be added to `spark-defaults.conf`:
| Property | Setting |
| -------- | ------- |
| spark.driver.extraClassPath | /usr/local/spark/jars/postgresql-42.2.14.jar |
| spark.driver.extraJavaOptions | -Dcom.amazonaws.services.s3.enableV4=true |
| spark.executor.extraJavaOptions | -Dcom.amazonaws.services.s3.enableV4=true |
| spark.hadoop.fs.s3a.awsAccessKeyId | $AWS_ACCESS_KEY_ID |
| spark.hadoop.fs.s3a.awsSecretAccessKey | $AWS_SECRET_ACCESS_KEY |
| spark.hadoop.fs.s3a.endpoint | $AWS_S3_ENDPOINT |
| spark.hadoop.com.amazonaws.services.s3a.enableV4 | true |
| spark.hadoop.fs.s3a.impl | org.apache.hadoop.fs.s3a.S3AFileSystem |
| spark.jars | /usr/local/spark/jars/postgresql-42.2.14.jar |
| spark.jars.packages | com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 |

This project also depends on PostgreSQL's PostGIS extension, which can be installed with the `CREATE EXTENSION` command:
```sh
psql -d yourdatabase -c 'CREATE EXTENSION postgis;'
```

## Directory Structure
```sh
.
├── LICENSE
├── README.md
├── dag.png
├── pipeline.png
├── requirements.txt
└── src/
    ├── airflow/
    │   ├── start_workers.sh*
    │   ├── stop_workers.sh*
    │   └── where_cycle_dag.py
    ├── config/
    │   ├── database.py
    │   ├── geometries.py
    │   ├── ref/
    │   │   ├── check_citibike_schema.py
    │   │   ├── check_tlc_schemas.py
    │   │   ├── get_geometries.sql
    │   │   └── tlc_schemas.txt
    │   └── schemas.py
    ├── dash/
    │   └── app.py
    ├── postGIS_tables/
    │   ├── geo_joined/
    │   │   ├── citibike_stations.sql
    │   │   └── past_tlc_visits.sql
    │   ├── production/
    │   │   ├── all_time_stats.sql
    │   │   └── taxi_zones.sql
    │   └── statistics/
    │       ├── citibike.sql
    │       ├── tlc_visits.sql
    │       └── yelp_businesses.sql
    ├── preparation/
    │   ├── extract.py
    │   ├── load.py
    │   └── transform.py
    └── spark_reduction/
        ├── driver.py
        ├── extract.py
        ├── load.py
        └── transform.py
```

## DAG Hierarchy
```sh
<Task(PythonOperator): get_taxi_zones>
    <Task(PythonOperator): clean_taxi_zones>
        <Task(PythonOperator): write_taxi_zones>
            <Task(BashOperator): create_production_taxi_zones>
    <Task(PythonOperator): calculate_centroids>
        <Task(PythonOperator): get_businesses>
            <Task(PythonOperator): clean_businesses>
                <Task(PythonOperator): write_businesses>
                    <Task(BashOperator): create_statistics_yelp_businesses>
                        <Task(BashOperator): create_production_all_time_stats>
<Task(PythonOperator): unzip_csvs>
    <Task(BashOperator): start_spark_workers>
        <Task(BashOperator): submit_spark_driver>
            <Task(BashOperator): stop_spark_workers>
            <Task(BashOperator): create_geo_joined_citibike_stations>
                <Task(BashOperator): create_statistics_citibike>
                    <Task(BashOperator): create_production_all_time_stats>
            <Task(BashOperator): create_geo_joined_past_tlc_visits>
                <Task(BashOperator): create_statistics_tlc_visits>
                    <Task(BashOperator): create_production_all_time_stats>
```

## License
[MIT License](LICENSE)<br />
Copyright (c) 2020 Josh Lang
