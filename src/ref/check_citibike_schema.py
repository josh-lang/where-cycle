import boto3
from pyspark.sql import SparkSession


s3 = boto3.resource('s3')
bucket = s3.Bucket('jlang-20b-de-ny')

spark = SparkSession.builder \
    .appName('citibike') \
    .getOrCreate()

for obj in bucket.objects.all():
    key = obj.key
    if key.startswith('citibike/') and key.endswith('.csv'):
        path = 's3a://jlang-20b-de-ny/' + key
        csv_df = spark.read.csv(
            path = path,
            header = True,
            inferSchema = True,
            enforceSchema = False,
            ignoreLeadingWhiteSpace = True,
            ignoreTrailingWhiteSpace = True,
            samplingRatio = 0.1
        )
        print(path)
        csv_df.printSchema()
