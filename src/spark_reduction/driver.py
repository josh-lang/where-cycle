from pyspark.sql import SparkSession
from spark_reduction.extract import \
    get_citibike_trips, get_past_tlc_trips, get_modern_tlc_trips
from spark_reduction.transform import \
    distill_citibike_stations, aggregate_citibike_visits, \
    aggregate_past_tlc_visits, aggregate_modern_tlc_visits
from spark_reduction.load import \
    write_citibike, write_tlc


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

# Parse CSVs from S3 and cache tables
get_citibike_trips()
get_past_tlc_trips()
get_modern_tlc_trips()

# Reduce tables to meaningful dataframes
stations = distill_citibike_stations()
citibike_visits = aggregate_citibike_visits()
past_visits = aggregate_past_tlc_visits()
modern_visits = aggregate_modern_tlc_visits()

# Write dataframes to postgres
write_citibike(stations, citibike_visits)
write_tlc(past_visits, modern_visits)

spark.stop()
