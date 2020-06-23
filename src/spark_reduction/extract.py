from pyspark.sql import SparkSession
from config.schemas import *


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

def get_citibike_trips():
    citibike_df = spark.read.csv(
        path = 's3a://jlang-20b-de-ny/citibike/*.csv',
        schema = citibike_schema,
        header = True,
        ignoreLeadingWhiteSpace = True,
        ignoreTrailingWhiteSpace = True
    ).withColumnRenamed('start station id', 'start_id') \
    .withColumnRenamed('start station latitude', 'start_latitude') \
    .withColumnRenamed('start station longitude', 'start_longitude') \
    .withColumnRenamed('end station id', 'end_id') \
    .withColumnRenamed('end station latitude', 'end_latitude') \
    .withColumnRenamed('end station longitude', 'end_longitude') \
    .selectExpr(
        'DATE_FORMAT(starttime, "yyyy-MM") AS start_month',
        'DATE_FORMAT(stoptime, "yyyy-MM") AS end_month',
        'start_id',
        'start_latitude',
        'start_longitude',
        'end_id',
        'end_latitude',
        'end_longitude'
    )

    citibike_df.createOrReplaceTempView('citibike')

def tlc_parse(path, schema):
    tlc_df = spark.read.csv(
        path = path,
        schema = schema,
        header = True,
        ignoreLeadingWhiteSpace = True,
        ignoreTrailingWhiteSpace = True
    ).withColumn(
        'month',
        regexp_extract(
            input_file_name(),
            r'tripdata_(\d{4}-\d{2})\.csv',
            1
        )
    )
    return tlc_df

def get_past_taxi_trips():
    past_df = spark.createDataFrame()
    green_paths = [
        's3a://nyc-tlc/trip\ data/green_tripdata_201[345]-*.csv',
        's3a://nyc-tlc/trip\ data/green_tripdata_2016-0[1-6].csv'
    ]
    for path in green_paths:
        green_df = tlc_parse(path, green_13_16_schema).selectExpr(
            'month',
            'Pickup_longitude AS pickup_longitude',
            'Pickup_latitude AS pickup_latitude',
            'Dropoff_longitude AS dropoff_longitude',
            'Dropoff_latitude AS dropoff_latitude'
        )

