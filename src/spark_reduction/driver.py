from pyspark.sql import SparkSession
from spark_reduction.extract import \
    get_citibike_trips, get_past_tlc_trips, get_modern_tlc_trips
from spark_reduction.transform_and_load import \
    citibike_stations, citibike_visits, past_tlc_visits, modern_tlc_visits


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

# Fetch and parse CSVs from S3
get_citibike_trips()
get_past_tlc_trips()
get_modern_tlc_trips()

# Reduce dataframes and write results to postgres
citibike_stations()
citibike_visits()
past_tlc_visits()
modern_tlc_visits()

spark.stop()
