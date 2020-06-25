from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from config.schemas import *


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

def get_citibike_trips():
    '''Parse Citibike CSVs, format date columns, & rename location columns'''
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

def parse_tlc(path, schema):
    '''Parse TLC CSVs, assuming trip month from filename'''
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
    '''Parse TLC CSVs from before 2016-07, filtering for lat-lon columns'''
    past_df = spark.createDataFrame(data = [], schema = past_schema)
    past_pairs = [
        ('s3a://nyc-tlc/trip\ data/green_tripdata_201[345]-*.csv', green_13_16_schema),
        ('s3a://nyc-tlc/trip\ data/green_tripdata_2016-0[1-6].csv', green_13_16_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_2009-*.csv', yellow_09_16_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_201[0-5]-*.csv', yellow_09_16_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[1-6].csv', yellow_09_16_schema)
    ]
    for path, schema in past_pairs:
        csv_df = parse_tlc(path, schema).select(
            'month',
            'pickup_longitude',
            'pickup_latitude',
            'dropoff_longitude',
            'dropoff_latitude'
        )
        past_df = past_df.union(csv_df)
    past_df.createOrReplaceTempView('past')

def get_modern_tlc_trips():
    '''Parse TLC CSVs from after 2016-06, filtering for taxi zone ID columns'''
    fhv_15_16_df = parse_tlc(
        's3a://nyc-tlc/trip\ data/fhv_tripdata_201[56]-*.csv',
        fhv_15_16_schema
    ).select(
        'month',
        'locationID'
    )
    fhv_15_16_df.createOrReplaceTempView('fhv_15_16')

    modern_df = spark.createDataFrame(data = [], schema = modern_schema)
    modern_pairs = [
        ('s3a://nyc-tlc/trip\ data/fhv_tripdata_201[79]-*.csv', fhv_17_19_schema),
        ('s3a://nyc-tlc/trip\ data/fhv_tripdata_2018-*.csv', fhv_18_schema),
        ('s3a://nyc-tlc/trip\ data/fhvhv_tripdata_*.csv', fhvhv_schema),
        ('s3a://nyc-tlc/trip\ data/green_tripdata_2016-0[789].csv', green_16_19_schema),
        ('s3a://nyc-tlc/trip\ data/green_tripdata_2016-1*.csv', green_16_19_schema),
        ('s3a://nyc-tlc/trip\ data/green_tripdata_201[789]-*.csv', green_16_19_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[789].csv', yellow_16_19_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_2016-1*.csv', yellow_16_19_schema),
        ('s3a://nyc-tlc/trip\ data/yellow_tripdata_201[789]-*.csv', yellow_16_19_schema)
    ]
    for path, schema in modern_pairs:
        csv_df = parse_tlc(path, schema).select(
            'month',
            'PULocationID',
            'DOLocationID'
        )
        modern_df = modern_df.union(csv_df)
    modern_df.createOrReplaceTempView('modern')
