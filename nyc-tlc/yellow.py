import boto3
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *


conf = (
    SparkConf().set(
        'spark.executor.extraJavaOptions',
        '-Dcom.amazonaws.services.s3.enableV4=true'
    ).set(
        'spark.driver.extraJavaOptions',
        '-Dcom.amazonaws.services.s3.enableV4=true'
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

sql = SparkSession(sc)

s3 = boto3.resource('s3')
nyc_tlc = s3.Bucket('nyc-tlc')

yellow_schema = StructType(
    [
        StructField('VendorID', StringType(), True),
        StructField('tpep_pickup_datetime', TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('Passenger_count', IntegerType(), True),
        StructField('Trip_distance', DoubleType(), True),
        StructField('PULocationID', IntegerType(), True),
        StructField('DOLocationID', IntegerType(), True),
        StructField('RateCodeID', StringType(), True),
        StructField('Store_and_fwd_flag', StringType(), True),
        StructField('Payment_type', StringType(), True),
        StructField('Fare_amount', DoubleType(), True),
        StructField('Extra', DoubleType(), True),
        StructField('MTA_tax', DoubleType(), True),
        StructField('Improvement_surcharge', DoubleType(), True),
        StructField('Tip_amount', DoubleType(), True),
        StructField('Tolls_amount', DoubleType(), True),
        StructField('Total_amount', DoubleType(), True)
    ]
)
yellow_df = sql.createDataFrame([], yellow_schema)

for obj in nyc_tlc.objects.all():
    key = obj.key
    if key.startswith('trip data/yellow') and key.endswith('.csv'):
        path = 's3a://nyc-tlc/' + key
        key_df = sql.read.csv(path, yellow_schema)
        yellow_df = yellow_df.union(key_df)

yellow_df.groupBy('PULocationID').count().show()
