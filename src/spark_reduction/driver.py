from pyspark.sql import SparkSession
from spark_reduction.extract import get_citibike_trips
from spark_reduction.transform_and_load import \
    citibike_stations_staging, citibike_visits

spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

get_citibike_trips()
citibike_stations_staging()
citibike_visits()

spark.stop()
