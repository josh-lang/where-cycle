import boto3
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


s3 = boto3.resource('s3')
nyc_tlc = s3.Bucket('nyc-tlc')

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

spark = SparkSession(sc)

for obj in nyc_tlc.objects.all():
    key = obj.key
    if key.startswith('trip data/') and key.endswith('.csv'):
        path = 's3a://nyc-tlc/' + key
        csv_df = spark.read.csv(
            path = path,
            header = True,
            inferSchema = True,
            enforceSchema = False,
            ignoreLeadingWhiteSpace = True,
            ignoreTrailingWhiteSpace = True,
            mode = 'DROPMALFORMED',
            samplingRatio = 0.1
        )
        print(path)
        csv_df.printSchema()
