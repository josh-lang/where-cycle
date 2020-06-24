from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from config.database import jdbc_props, jdbc_url
from config.schemas import green_13_16_schema, yellow_09_16_schema


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

green_13_16_0_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/green_tripdata_201[345]-*.csv',
    schema = green_13_16_schema,
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
).selectExpr(
    'month',
    'Pickup_longitude AS pickup_longitude',
    'Pickup_latitude AS pickup_latitude',
    'Dropoff_longitude AS dropoff_longitude',
    'Dropoff_latitude AS dropoff_latitude'
)

green_13_16_1_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/green_tripdata_2016-0[1-6].csv',
    schema = green_13_16_schema,
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
).selectExpr(
    'month',
    'Pickup_longitude AS pickup_longitude',
    'Pickup_latitude AS pickup_latitude',
    'Dropoff_longitude AS dropoff_longitude',
    'Dropoff_latitude AS dropoff_latitude'
)

past_df = green_13_16_0_df.union(green_13_16_1_df)

yellow_09_16_0_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2009-*.csv',
    schema = yellow_09_16_schema,
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
    'pickup_longitude',
    'pickup_latitude',
    'dropoff_longitude',
    'dropoff_latitude'
)

past_df = past_df.union(yellow_09_16_0_df)

yellow_09_16_1_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_201[0-5]-*.csv',
    schema = yellow_09_16_schema,
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
    'pickup_longitude',
    'pickup_latitude',
    'dropoff_longitude',
    'dropoff_latitude'
)

past_df = past_df.union(yellow_09_16_1_df)

yellow_09_16_2_df = spark.read.csv(
    path = 's3a://nyc-tlc/trip\ data/yellow_tripdata_2016-0[1-6].csv',
    schema = yellow_09_16_schema,
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
    'pickup_longitude',
    'pickup_latitude',
    'dropoff_longitude',
    'dropoff_latitude'
)

past_df = past_df.union(yellow_09_16_2_df)

past_df.createOrReplaceTempView('past')

past_writable = spark.sql('''
    SELECT
        month,
        longitude,
        latitude,
        SUM(visits) AS visits
    FROM (
        SELECT
            month,
            ROUND(pickup_longitude, 3) AS longitude,
            ROUND(pickup_latitude, 3) AS latitude,
            COUNT(*) AS visits
        FROM past
        WHERE
            pickup_longitude BETWEEN -74.2555913631521 AND -73.7000090639354
            AND
            pickup_latitude BETWEEN 40.4961153951704 AND 40.9155327770026
        GROUP BY 1, 2, 3
        UNION ALL
        SELECT
            month,
            ROUND(dropoff_longitude, 3) AS longitude,
            ROUND(dropoff_latitude, 3) AS latitude,
            COUNT(*) AS visits
        FROM past
        WHERE
            dropoff_longitude BETWEEN -74.2555913631521 AND -73.7000090639354
            AND
            dropoff_latitude BETWEEN 40.4961153951704 AND 40.9155327770026
        GROUP BY 1, 2, 3
    )
    GROUP BY 1, 2, 3
''')

past_writable.write.jdbc(
    url = jdbc_url,
    table = 'staging.past_tlc_visits',
    mode = 'overwrite',
    properties = jdbc_props
)

spark.stop()
