from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from preparation.extract import \
    unzip_csvs, get_taxi_zones, get_businesses
from preparation.transform import \
    calculate_centroids, clean_businesses, clean_taxi_zones
from preparation.load import \
    write_businesses, write_taxi_zones


airflow_path = '/home/ubuntu/where-cycle/src/airflow/'
spark_str = 'cd /home/ubuntu/where-cycle/src/spark_reduction && '
psql_str = 'psql -h $PSQL_HOST -p $PSQL_PORT -U $PSQL_USER -d ' + \
    '$PSQL_DATABASE -f /home/ubuntu/where-cycle/src/postGIS_tables/'

defaults = {
    'owner': 'airflow',
    'start_date': datetime(2020, 6, 21),
    'depends_on_past': False
    # 'retries': 2,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
    'where_cycle',
    default_args = defaults,
    schedule_interval = '@weekly'
) as dag:
    #********    PREPARATION    ********#

    t1 = PythonOperator(
        task_id = 'unzip_csvs',
        python_callable = unzip_csvs
    )

    t2 = PythonOperator(
        task_id = 'get_taxi_zones',
        python_callable = get_taxi_zones
    )

    t3 = PythonOperator(
        task_id = 'calculate_centroids',
        python_callable = calculate_centroids,
        provide_context = True
    )

    t4 = PythonOperator(
        task_id = 'get_businesses',
        python_callable = get_businesses,
        provide_context = True
    )

    t5 = PythonOperator(
        task_id = 'clean_businesses',
        python_callable = clean_businesses,
        provide_context = True
    )

    t6 = PythonOperator(
        task_id = 'clean_taxi_zones',
        python_callable = clean_taxi_zones,
        provide_context = True
    )

    t7 = PythonOperator(
        task_id = 'write_businesses',
        python_callable = write_businesses,
        provide_context = True
    )

    t8 = PythonOperator(
        task_id = 'write_taxi_zones',
        python_callable = write_taxi_zones,
        provide_context = True
    )

    t2 >> t3 >> t4 >> t5 >> t7
    t2 >> t6 >> t8


    #********    SPARK REDUCTION    ********#

    t9 = BashOperator(
        task_id = 'start_spark_workers',
        bash_command = airflow_path + 'start_workers.sh '
    )

    t10 = BashOperator(
        task_id = 'spark_submit_driver',
        bash_command = spark_str + 'spark-submit driver.py'
    )

    t11 = BashOperator(
        task_id = 'stop_spark_workers',
        bash_command = airflow_path + 'stop_workers.sh ',
        trigger_rule = 'all_done'
    )

    t1 >> t9 >> t10 >> t11


    #********    POSTGIS TABLES    ********#

    t12 = BashOperator(
        task_id = 'geo_joined_citibike_stations',
        bash_command = psql_str + 'geo_joined/citibike_stations.sql'
    )

    t13 = BashOperator(
        task_id = 'statistics_citibike',
        bash_command = psql_str + 'statistics/citibike.sql'
    )

    t14 = BashOperator(
        task_id = 'statistics_yelp_businesses',
        bash_command = psql_str + 'statistics/yelp_businesses.sql'
    )

    t15 = BashOperator(
        task_id = 'production_all_time_stats',
        bash_command = psql_str + 'production/all_time_stats.sql'
    )

    t7 >> t14 >> t15
    [t8, t10] >> t12 >> t13 >> t15
