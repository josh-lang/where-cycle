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
)

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
            count(*) AS trips
        FROM csv
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            DOLocationID AS zone_id,
            count(*) AS trips
        FROM csv
        GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1, 2
''')

jdbc_url = 'jdbc:postgresql://' + \
    os.environ['PSQL_HOST'] + ':' + os.environ['PSQL_PORT'] + \
    '/' + os.environ['PSQL_DATABASE']

endpoints.write.jdbc(
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
