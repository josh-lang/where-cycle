# Where Cycle

![](https://img.shields.io/badge/python-3-brightgreen.svg)

*Getting New Yorkers Back to Business, Safely*

## Contents
1. [Purpose](README.md#purpose)
1. [Pipeline](README.md#pipeline)
1. [Summary](README.md#summary)
1. [Setup](README.md#setup)
1. [Directory Structure](README.md#directory-structure)
1. [DAG Hierarchy](README.md#dag-hierarchy)
1. [License](README.md#license)

## Purpose
With businesses closing and health officials advising social distancing earlier this year, it’s no surprise that subway and bus ridership plummeted in many large cities, including New York, which saw an almost 90% reduction by late April. Now, as cities are tentatively opening back up, people may be looking to return to their places of work and to support their favorite businesses, but they might also be hesitant to utilize public transit, instead seeking open-air alternatives.

A cursory glance at some transit coverage in the city makes it clear that, while Citibike is an awesome open-air solution, the available stations can’t immediately meet the needs of the outer boroughs: some expansion is required. The goal of this pipeline is to determine which NYC taxi zones may be the best candidates for Citibike expansion by aggregating historical taxi & for-hire vehicle trips, Citibike stations already present, and Yelp business statistics.

## Pipeline
![Preview](https://github.com/josh-lang/where-cycle/blob/master/pipeline.png)

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
MIT License

Copyright (c) 2020 Josh Lang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
