from pyspark.sql import SparkSession
from spark_reduction.extract import \
    get_citibike_trips, get_past_tlc_trips, get_modern_tlc_trips
from spark_reduction.transform_and_load import \
    citibike_stations, citibike_visits, past_tlc_visits, modern_tlc_visits

spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

# get_citibike_trips()
# citibike_stations()
# citibike_visits()

get_past_tlc_trips()
past_tlc_visits()

get_modern_tlc_trips()
modern_tlc_visits()

spark.stop()
