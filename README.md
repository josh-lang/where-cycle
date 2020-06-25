![Python 3.6.9](https://img.shields.io/badge/Python-3.6.9-light)
![Airflow 1.10.10](https://img.shields.io/badge/Airflow-1.10.10-red)
![GeoPandas 0.7](https://img.shields.io/badge/GeoPandas-0.7-purple)
![Spark 2.4.5](https://img.shields.io/badge/Spark-2.4.5-orange)
![PostGIS 2.4](https://img.shields.io/badge/PostGIS-2.4-darkblue)
![Dash 1.12](https://img.shields.io/badge/Dash-1.12-blue)
![MIT License](https://img.shields.io/badge/license-MIT-lightgrey)
# Where Cycle

*Getting New Yorkers Back to Business, Safely*

<!-- 1. [Summary](README.md#summary) -->
## Contents
1. [Purpose](README.md#purpose)
1. [Pipeline](README.md#pipeline)
1. [Data](README.md#data)
    <!-- - [Preparation](README.md#preparation)
    - [Spark Reduction](README.md#spark-reduction)
    - [postGIS Tables](README.md#postgis-tables) -->
1. [Setup](README.md#setup)
1. [Directory Structure](README.md#directory-structure)
1. [DAG Hierarchy](README.md#dag-hierarchy)
1. [License](README.md#license)

## Purpose
As health officials advised social distancing and businesses closed earlier this year, subway and bus ridership plummeted in many large cities including New York, which saw an almost 90% reduction by late April. Now, as cities are tentatively opening back up, people may be looking to return to their places of work and to support their favorite businesses, but they might be hesitant to utilize public transit, instead seeking open-air alternatives.

A cursory glance at some transit coverage in NYC makes it clear that, while Citibike is an awesome open-air solution, the available stations can’t immediately meet the needs of the outer boroughs: some expansion is required. **The goal of this pipeline is to determine which NYC taxi zones may be the best candidates for Citibike expansion by aggregating historical taxi & for-hire vehicle trips, Citibike trips & station density, and Yelp business statistics.**

*This project was developed by Josh Lang as part of his data engineering fellowship with Insight Data Science in the summer of 2020.*

## Pipeline
![Pipeline](https://github.com/josh-lang/where-cycle/blob/master/pipeline.png)
![DAG](https://github.com/josh-lang/where-cycle/blob/master/dag.png)

<!-- ## Summary  -->
## Data
 - Citibike Trip Histories: [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/tripdata), [documentation](https://www.citibikenyc.com/system-data)
 - NYC Taxi & Limousine Commission Trip Records: [S3 bucket](https://s3.console.aws.amazon.com/s3/buckets/nyc-tlc), [documentation](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
 - Yelp Business Search API: [documentation](https://www.yelp.com/developers/documentation/v3/business_search)
<!-- ### Preparation
 - Spark can't ingest zip files natively since Hadoop, which provides the underlying filesystem interface, does not support that compression codec, so Citibike's zipped files need to be 
### Spark Reduction
TODO
### PostGIS Tables
TODO -->

## Setup
The python dependencies can be installed with the following command:
```
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
```
psql -d yourdatabase -c 'CREATE EXTENSION postgis;'
```

## Directory Structure
```
.
├── LICENSE
├── README.md
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
    │   │   ├── check_taxi_schemas.py
    │   │   └── taxi_schemas.txt
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
        └── transform_and_load.py
```

## DAG Hierarchy
```
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
