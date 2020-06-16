from db_config import jdbc_props, jdbc_url
from pyspark.sql import SparkSession
from schemas import citibike_schema


spark = SparkSession.builder \
    .appName('citibike_stations_visits') \
    .getOrCreate()

citibike_df = spark.read.csv(
    path = 's3a://jlang-20b-de-ny/citibike/*.csv',
    schema = citibike_schema,
    header = True,
    ignoreLeadingWhiteSpace = True,
    ignoreTrailingWhiteSpace = True
).withColumnRenamed('start station id', 'start_id') \
    .withColumnRenamed('start station latitude', 'start_latitude') \
    .withColumnRenamed('start station longitude', 'start_longitude') \
    .withColumnRenamed('end station id', 'end_id') \
    .withColumnRenamed('end station latitude', 'end_latitude') \
    .withColumnRenamed('end station longitude', 'end_longitude') \
    .selectExpr(
        'DATE_FORMAT(starttime, "yyyy-MM") AS start_month',
        'DATE_FORMAT(stoptime, "yyyy-MM") AS end_month',
        'start_id',
        'start_latitude',
        'start_longitude',
        'end_id',
        'end_latitude',
        'end_longitude'
    )

citibike_df.createOrReplaceTempView('citibike')

citibike_stations = spark.sql('''
    SELECT
        start_id AS station_id,
        start_latitude AS latitude,
        start_longitude AS longitude
    FROM citibike
    WHERE
        start_latitude BETWEEN 40.4961153951704 AND 40.9155327770026
        AND
        start_longitude BETWEEN -74.2555913631521 AND -73.7000090639354
    GROUP BY 1, 2, 3
    UNION
    SELECT
        end_id AS station_id,
        end_latitude AS latitude,
        end_longitude AS longitude
    FROM citibike
    WHERE
        end_latitude BETWEEN 40.4961153951704 AND 40.9155327770026
        AND
        end_longitude BETWEEN -74.2555913631521 AND -73.7000090639354
    GROUP BY 1, 2, 3
''')

citibike_endpoint_visits = spark.sql('''
    SELECT
        month,
        station_id,
        SUM(endpoint_visits) AS endpoint_visits
    FROM (
        SELECT
            start_month AS month,
            start_id AS station_id,
            COUNT(*) AS endpoint_visits
        FROM citibike
        GROUP BY 1, 2
        UNION ALL
        SELECT
            end_month AS month,
            end_id AS station_id,
            COUNT(*) AS endpoint_visits
        FROM citibike
        GROUP BY 1, 2
    )
    GROUP BY 1, 2
    ORDER BY 1, 2
''')

citibike_stations.write.jdbc(
    url = jdbc_url,
    table = 'citibike_stations_staging',
    mode = 'overwrite',
    properties = jdbc_props
)

citibike_endpoint_visits.write.jdbc(
    url = jdbc_url,
    table = 'citibike_endpoint_visits',
    mode = 'overwrite',
    properties = jdbc_props
)

spark.stop()
