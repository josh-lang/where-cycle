import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = (
    SparkConf().set(
        'spark.executor.extraJavaOptions',
        '-Dcom.amazonaws.services.s3.enableV4=true'
    ).set(
        'spark.driver.extraJavaOptions',
        '-Dcom.amazonaws.services.s3.enableV4=true'
    ).set(
        'spark.driver.extraClassPath',
        'postgresql-9.4.1212.jar'
    ).set(
        'spark.jars',
        'postgresql-9.4.1212.jar'
    ).set(
        'spark.jars.packages',
        'com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7'
    )
)

sc = SparkContext(conf=conf)
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.awsAccessKeyId', os.environ['AWS_ACCESS_KEY_ID'])
hadoopConf.set('fs.s3a.awsSecretAccessKey', os.environ['AWS_SECRET_ACCESS_KEY'])
hadoopConf.set('fs.s3a.endpoint', 's3.us-east-1.amazonaws.com')
hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark = SparkSession(sc)

from pyspark.sql.types import StructType, StructField, \
    IntegerType, TimestampType, StringType, DoubleType

# green_13_16_schema = StructType(
#     [
#         StructField('VendorID', IntegerType(), True),
#         StructField('lpep_pickup_datetime', TimestampType(), True),
#         StructField('Lpep_dropoff_datetime', TimestampType(), True),
#         StructField('Store_and_fwd_flag', StringType(), True),
#         StructField('RateCodeID', IntegerType(), True),
#         StructField('Pickup_longitude', DoubleType(), True),
#         StructField('Pickup_latitude', DoubleType(), True),
#         StructField('Dropoff_longitude', DoubleType(), True),
#         StructField('Dropoff_latitude', DoubleType(), True)
#     ]
# )

# yellow_09_16_schema = StructType(
#     [
#         StructField('VendorID', StringType(), True),
#         StructField('tpep_pickup_datetime', TimestampType(), True),
#         StructField('tpep_dropoff_datetime', TimestampType(), True),
#         StructField('passenger_count', IntegerType(), True),
#         StructField('trip_distance', DoubleType(), True),
#         StructField('pickup_longitude', DoubleType(), True),
#         StructField('pickup_latitude', DoubleType(), True),
#         StructField('RateCodeID', StringType(), True),
#         StructField('store_and_fwd_flag', StringType(), True),
#         StructField('dropoff_longitude', DoubleType(), True),
#         StructField('dropoff_latitude', DoubleType(), True)
#     ]
# )

from pyspark.sql.functions import regexp_extract, input_file_name

# fhv_15_16_schema = StructType(
#     [
#         StructField('Dispatching_base_num', StringType(), True),
#         StructField('Pickup_date', TimestampType(), True),
#         StructField('locationID', IntegerType(), True)
#     ]
# )

# fhv_17_19_schema = StructType(
#     [
#         StructField('Dispatching_base_num', StringType(), True),
#         StructField('Pickup_DateTime', TimestampType(), True),
#         StructField('DropOff_datetime', TimestampType(), True),
#         StructField('PUlocationID', IntegerType(), True),
#         StructField('DOlocationID', IntegerType(), True)
#     ]
# )

# fhv_18_schema = StructType(
#     [
#         StructField('Pickup_DateTime', TimestampType(), True),
#         StructField('DropOff_datetime', TimestampType(), True),
#         StructField('PUlocationID', IntegerType(), True),
#         StructField('DOlocationID', IntegerType(), True)
#     ]
# )

# fhvhv_schema = StructType(
#     [
#         StructField('hvfhs_license_num', StringType(), True),
#         StructField('dispatching_base_num', StringType(), True),
#         StructField('pickup_datetime', TimestampType(), True),
#         StructField('dropoff_datetime', TimestampType(), True),
#         StructField('PULocationID', IntegerType(), True),
#         StructField('DOLocationID', IntegerType(), True)
#     ]
# )

green_16_19_schema = StructType(
    [
        StructField('VendorID', IntegerType(), True),
        StructField('lpep_pickup_datetime', TimestampType(), True),
        StructField('lpep_dropoff_datetime', TimestampType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)

yellow_16_19_schema = StructType(
    [
        StructField('VendorID', IntegerType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('passenger_count', IntegerType(), True),
        StructField('trip_distance', DoubleType(), True),
        StructField('RatecodeID', IntegerType(), True),
        StructField('store_and_fwd_flag', StringType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)

# green_16_19_0_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/green_tripdata_2016-0[789].csv',
#     schema = green_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# green_16_19_1_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/green_tripdata_2016-1*.csv',
#     schema = green_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# csv_df = green_16_19_0_df.union(green_16_19_1_df)

# green_16_19_2_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/green_tripdata_201[789]-*.csv',
#     schema = green_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# csv_df = csv_df.union(green_16_19_2_df)

# yellow_16_19_0_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[789].csv',
#     schema = yellow_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# csv_df = csv_df.union(yellow_16_19_0_df)

# yellow_16_19_1_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-1*.csv',
#     schema = yellow_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# csv_df = csv_df.union(yellow_16_19_1_df)

# yellow_16_19_2_df = spark.read.csv(
#     path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_201[789]-*.csv',
#     schema = yellow_16_19_schema,
#     header = True,
#     ignoreLeadingWhiteSpace = True,
#     ignoreTrailingWhiteSpace = True
# ).withColumn(
#     'month',
#     regexp_extract(
#         input_file_name(),
#         r'tripdata_(\d{4}-\d{2})\.csv',
#         1
#     )
# ).select(
#     'month',
#     'PULocationID',
#     'DOLocationID'
# )

# csv_df = csv_df.union(yellow_16_19_2_df)

csv_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2019-12.csv',
    schema = yellow_16_19_schema,
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
).select(
    'month',
    'PULocationID',
    'DOLocationID'
).limit(1000)

csv_df.createOrReplaceTempView('csv')
endpoints = spark.sql('''
    SELECT
        month,
        zone_id,
        sum(trips) / 2 AS trips
    FROM (
        SELECT
            month,
            PULocationID AS zone_id,
            count(*) as trips
        FROM csv
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            DOLocationID AS zone_id,
            count(*) as trips
        FROM csv
        GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1, 2
''')

import pandas as pd
from sqlalchemy import *


db_url = 'postgresql://' + \
    os.environ['PSQL_USER'] + ':' + os.environ['PSQL_PASSWORD'] + \
    '@' + os.environ['PSQL_HOST'] + ':' + os.environ['PSQL_PORT'] + \
    '/' + os.environ['PSQL_DATABASE']
# engine = create_engine(db_url)

modern_taxi_trips = endpoints.toPandas()

modern_taxi_trips.to_sql(
    name = 'modern_taxi_trips',
    con = engine,
    if_exists = 'replace',
#     index = False,
#     index_label = 'zone_id',
    dtype = {
        'month': String(length = 7),
        'zone_id': Integer(),
        'trips': Float(1)
    }
)

# endpoints.write.jdbc(
#     url = db_url,
#     table='modern_taxi_trips',
#     mode='overwrite'
# )

spark.stop()
