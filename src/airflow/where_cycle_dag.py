from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from preparation.extract import \
    get_taxi_zones, get_businesses, unzip_csvs
from preparation.transform import \
    clean_taxi_zones, calculate_centroids, clean_businesses
from preparation.load import \
    write_taxi_zones, write_businesses


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
        task_id = 'get_taxi_zones',
        python_callable = get_taxi_zones
    )

    t2 = PythonOperator(
        task_id = 'clean_taxi_zones',
        python_callable = clean_taxi_zones,
        provide_context = True
    )

    t3 = PythonOperator(
        task_id = 'write_taxi_zones',
        python_callable = write_taxi_zones,
        provide_context = True
    )

    t4 = PythonOperator(
        task_id = 'calculate_centroids',
        python_callable = calculate_centroids,
        provide_context = True
    )

    t5 = PythonOperator(
        task_id = 'get_businesses',
        python_callable = get_businesses,
        provide_context = True
    )

    t6 = PythonOperator(
        task_id = 'clean_businesses',
        python_callable = clean_businesses,
        provide_context = True
    )

    t7 = PythonOperator(
        task_id = 'write_businesses',
        python_callable = write_businesses,
        provide_context = True
    )

    t8 = PythonOperator(
        task_id = 'unzip_csvs',
        python_callable = unzip_csvs
    )

    t1 >> t2 >> t3
    t1 >> t4 >> t5 >> t6 >> t7


    #********    SPARK REDUCTION    ********#

    t9 = BashOperator(
        task_id = 'start_spark_workers',
        bash_command = airflow_path + 'start_workers.sh '
    )

    t10 = BashOperator(
        task_id = 'submit_spark_driver',
        bash_command = spark_str + 'spark-submit driver.py'
    )

    t11 = BashOperator(
        task_id = 'stop_spark_workers',
        bash_command = airflow_path + 'stop_workers.sh ',
        trigger_rule = 'all_done'
    )

    t8 >> t9 >> t10 >> t11


    #********    POSTGIS TABLES    ********#

    t12 = BashOperator(
        task_id = 'create_production_taxi_zones',
        bash_command = psql_str + 'production/taxi_zones.sql'
    )
    
    t13 = BashOperator(
        task_id = 'create_statistics_yelp_businesses',
        bash_command = psql_str + 'statistics/yelp_businesses.sql'
    )

    t14 = BashOperator(
        task_id = 'create_geo_joined_citibike_stations',
        bash_command = psql_str + 'geo_joined/citibike_stations.sql'
    )

    t15 = BashOperator(
        task_id = 'create_statistics_citibike',
        bash_command = psql_str + 'statistics/citibike.sql'
    )

    t16 = BashOperator(
        task_id = 'create_geo_joined_past_tlc_visits',
        bash_command = psql_str + 'geo_joined/past_tlc_visits.sql'
    )

    t17 = BashOperator(
        task_id = 'create_statistics_tlc_visits',
        bash_command = psql_str + 'statistics/tlc_visits.sql'
    )

    t18 = BashOperator(
        task_id = 'create_production_all_time_stats',
        bash_command = psql_str + 'production/all_time_stats.sql'
    )

    t3 >> t12
    t7 >> t13
    t10 >> t14 >> t15
    t10 >> t16 >> t17
    [t13, t15, t17] >> t18
