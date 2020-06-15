import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name
from pyspark.sql.types import StructType, StructField, \
    IntegerType, TimestampType, StringType, DoubleType

sc = SparkContext()
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.awsAccessKeyId', os.environ['AWS_ACCESS_KEY_ID'])
hadoopConf.set('fs.s3a.awsSecretAccessKey', os.environ['AWS_SECRET_ACCESS_KEY'])
hadoopConf.set('fs.s3a.endpoint', 's3.us-east-1.amazonaws.com')
hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark = SparkSession(sc)

fhv_15_16_schema = StructType(
    [
        StructField('Dispatching_base_num', StringType(), True),
        StructField('Pickup_date', TimestampType(), True),
        StructField('locationID', IntegerType(), True)
    ]
)

fhv_17_19_schema = StructType(
    [
        StructField('Dispatching_base_num', StringType(), True),
        StructField('Pickup_DateTime', TimestampType(), True),
        StructField('DropOff_datetime', TimestampType(), True),
        StructField('PUlocationID', IntegerType(), True),
        StructField('DOlocationID', IntegerType(), True)
    ]
)

fhv_18_schema = StructType(
    [
        StructField('Pickup_DateTime', TimestampType(), True),
        StructField('DropOff_datetime', TimestampType(), True),
        StructField('PUlocationID', IntegerType(), True),
        StructField('DOlocationID', IntegerType(), True)
    ]
)

fhvhv_schema = StructType(
    [
        StructField('hvfhs_license_num', StringType(), True),
        StructField('dispatching_base_num', StringType(), True),
        StructField('pickup_datetime', TimestampType(), True),
        StructField('dropoff_datetime', TimestampType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True)
    ]
)

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

fhv_15_16_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/fhv_tripdata_201[56]-*.csv',
    schema = fhv_15_16_schema,
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
    'locationID'
)

fhv_17_19_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/fhv_tripdata_201[79]-*.csv',
    schema = fhv_17_19_schema,
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
)

fhv_18_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/fhv_tripdata_2018-*.csv',
    schema = fhv_18_schema,
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
)

modern_df = fhv_17_19_df.union(fhv_18_df)

fhvhv_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/fhvhv_tripdata_*.csv',
    schema = fhvhv_schema,
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
)

modern_df = modern_df.union(fhvhv_df)

green_16_19_0_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/green_tripdata_2016-0[789].csv',
    schema = green_16_19_schema,
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
)

modern_df = modern_df.union(green_16_19_0_df)

green_16_19_1_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/green_tripdata_2016-1*.csv',
    schema = green_16_19_schema,
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
)

modern_df = modern_df.union(green_16_19_1_df)

green_16_19_2_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/green_tripdata_201[789]-*.csv',
    schema = green_16_19_schema,
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
)

modern_df = modern_df.union(green_16_19_2_df)

yellow_16_19_0_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[789].csv',
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
)

modern_df = modern_df.union(yellow_16_19_0_df)

yellow_16_19_1_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-1*.csv',
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
)

modern_df = modern_df.union(yellow_16_19_1_df)

yellow_16_19_2_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_201[789]-*.csv',
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
)

modern_df = modern_df.union(yellow_16_19_2_df)

fhv_15_16_df.createOrReplaceTempView('fhv_15_16')
modern_df.createOrReplaceTempView('modern')

modern_writable = spark.sql('''
    SELECT
        month,
        zone_id,
        CAST(SUM(trips) AS DOUBLE) / 2 AS trips
    FROM (
        SELECT
            month,
            locationID AS zone_id,
            COUNT(*) AS trips
        FROM fhv_15_16
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            PULocationID AS zone_id,
            COUNT(*) as trips
        FROM modern
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            DOLocationID AS zone_id,
            COUNT(*) as trips
        FROM modern
        GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1, 2
''')

jdbc_url = 'jdbc:postgresql://' + \
    os.environ['PSQL_HOST'] + ':' + os.environ['PSQL_PORT'] + \
    '/' + os.environ['PSQL_DATABASE']

modern_writable.write.jdbc(
    url = jdbc_url,
    table = 'modern_taxi_trips',
    mode = 'overwrite',
    properties = {
        'driver': 'org.postgresql.Driver',
        'user': os.environ['PSQL_USER'],
        'password': os.environ['PSQL_PASSWORD']
    }
)

spark.stop()
