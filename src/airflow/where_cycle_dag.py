from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from preparation.extract \
    import unzip_csvs, get_taxi_zones, get_businesses
from preparation.transform \
    import calculate_centroids, clean_businesses, clean_taxi_zones
from preparation.load \
    import write_businesses, write_taxi_zones


defaults = {
    'owner': 'airflow',
    'start_date': datetime(2020, 6, 14),
    'depends_on_past': False,
    'email': ['josh@dats.work'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@weekly'
}
with DAG('where-cycle', default_args = defaults) as dag:
    #####     PREPARATION     #####
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

    #####     REDUCTION     #####

    t9 = BashOperator(
        task_id = 'start_spark_workers',
        bash_command = "/home/ubuntu/where-cycle/src/airflow/start_workers.sh "
    )

    
