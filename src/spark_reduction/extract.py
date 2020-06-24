from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
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

def get_past_tlc_trips():
    past_df = spark.createDataFrame(data = [], schema = past_schema)

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
        past_df = past_df.union(green_df)

    yellow_paths = [
        's3a://nyc-tlc/trip\ data/yellow_tripdata_2009-*.csv',
        's3a://nyc-tlc/trip\ data/yellow_tripdata_201[0-5]-*.csv',
        's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[1-6].csv'
    ]
    for path in yellow_paths:
        yellow_df = tlc_parse(path, yellow_09_16_schema).select(
            'month',
            'pickup_longitude',
            'pickup_latitude',
            'dropoff_longitude',
            'dropoff_latitude'
        )
        past_df = past_df.union(yellow_df)

    past_df.createOrReplaceTempView('past')

