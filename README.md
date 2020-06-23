# Where Cycle
*Getting New Yorkers Back to Business, Safely*
![](https://img.shields.io/badge/python-3-brightgreen.svg)

## Contents
1. [Purpose](README.md#purpose)
1. [Pipeline](README.md#pipeline)
1. [Summary](README.md#summary)
1. [Setup](README.md#setup)
1. [Directory Structure](README.md#directory-structure)
1. [DAG Hierarchy](README.md#dag-hierarchy)
1. [License](README.md#license)

## Goal

## Pipeline
![Preview](https://github.com/josh-lang/where-cycle/pipeline.png)

## Summary

## Setup
```sh
pip install -r requirements.txt
```

## Directory Structure
```
.
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
    │   ├── ref/
    │   │   ├── check_citibike_schema.py
    │   │   ├── check_taxi_schemas.py
    │   │   └── taxi_schemas.txt
    │   └── schemas.py
    ├── dash/
    │   └── app.py
    ├── postGIS_tables/
    │   ├── geometries/
    │   │   ├── citibike_stations.sql
    │   │   └── past_taxi_endpoint_visits.sql
    │   ├── production/
    │   │   ├── all_time_stats_production.sql
    │   │   └── taxi_zones_production.sql
    │   └── statistics/
    │       ├── citibike_stats.sql
    │       ├── taxi_endpoint_visits.sql
    │       └── yelp_stats.sql
    ├── preparation/
    │   ├── extract.py
    │   ├── load.py
    │   └── transform.py
    └── spark_reduction/
        ├── citibike_stations_visits.py
        ├── extract.py
        ├── modern_taxi_visits.py
        └── past_taxi_visits_staging.py
```

## DAG Hierarchy
```
<Task(PythonOperator): unzip_csvs>
<Task(PythonOperator): get_taxi_zones>
    <Task(PythonOperator): clean_taxi_zones>
        <Task(PythonOperator): write_taxi_zones>
    <Task(PythonOperator): calculate_centroids>
        <Task(PythonOperator): get_businesses>
            <Task(PythonOperator): clean_businesses>
                <Task(PythonOperator): write_businesses>
<Task(BashOperator): start_spark_workers>
```

## License
