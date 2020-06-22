from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from config.database import jdbc_props, jdbc_url
from config.schemas import \
    fhv_15_16_schema, fhv_17_19_schema, \
    fhv_18_schema, fhvhv_schema, \
    green_16_19_schema, yellow_16_19_schema


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

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
        SUM(visits) AS visits
    FROM (
        SELECT
            month,
            locationID AS zone_id,
            COUNT(*) AS visits
        FROM fhv_15_16
        WHERE locationID BETWEEN 1 AND 263
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            PULocationID AS zone_id,
            COUNT(*) as visits
        FROM modern
        WHERE PUlocationID BETWEEN 1 AND 263
        GROUP BY 1, 2
        UNION ALL
        SELECT
            month,
            DOLocationID AS zone_id,
            COUNT(*) as visits
        FROM modern
        WHERE DOlocationID BETWEEN 1 AND 263
        GROUP BY 1, 2
    )
    GROUP BY 1, 2
''')

modern_writable.write.jdbc(
    url = jdbc_url,
    table = 'modern_taxi_visits',
    mode = 'overwrite',
    properties = jdbc_props
)

spark.stop()
