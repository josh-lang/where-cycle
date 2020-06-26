import boto3
from pyspark.sql import SparkSession


s3 = boto3.resource('s3')
nyc_tlc = s3.Bucket('nyc-tlc')

spark = SparkSession.builder \
    .appName('check_tlc_schemas') \
    .getOrCreate()

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
            samplingRatio = 0.1
        )
        print(path)
        csv_df.printSchema()
