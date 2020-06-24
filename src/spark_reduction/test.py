from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from config.schemas import *

spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

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



get_modern_tlc_trips()

modern_writable = spark.sql('''
    SELECT COUNT(*) FROM modern
''')

modern_writable.show()

spark.stop()
