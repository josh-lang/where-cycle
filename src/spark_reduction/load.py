from pyspark.sql import SparkSession
from config.database import jdbc_props, jdbc_url


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

def write_citibike(stations, visits):
    stations.write.jdbc(
        url = jdbc_url,
        table = 'staging.citibike_stations',
        mode = 'overwrite',
        properties = jdbc_props
    )
    visits.write.jdbc(
        url = jdbc_url,
        table = 'staging.citibike_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )
    spark.catalog.uncacheTable('citibike')

def write_tlc(past, modern):
    past.write.jdbc(
        url = jdbc_url,
        table = 'staging.past_tlc_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )
    spark.catalog.uncacheTable('past')

    modern.write.jdbc(
        url = jdbc_url,
        table = 'staging.modern_tlc_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )
    spark.catalog.uncacheTable('fhv_15_16')
    spark.catalog.uncacheTable('modern')
