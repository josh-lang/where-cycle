from pyspark.sql import SparkSession
from config.database import jdbc_props, jdbc_url
from config.geometries import \
    TAXI_ZONE_LAT_MIN, TAXI_ZONE_LAT_MAX, TAXI_ZONE_LON_MIN, TAXI_ZONE_LON_MAX


spark = SparkSession.builder \
    .appName('where-cycle') \
    .getOrCreate()

def citibike_stations():
    stations = spark.sql(f'''
        SELECT
            start_id AS station_id,
            start_latitude AS latitude,
            start_longitude AS longitude
        FROM citibike
        WHERE
            start_latitude BETWEEN {TAXI_ZONE_LAT_MIN} AND {TAXI_ZONE_LAT_MAX}
            AND
            start_longitude BETWEEN {TAXI_ZONE_LON_MIN} AND {TAXI_ZONE_LON_MAX}
        GROUP BY 1, 2, 3
        UNION
        SELECT
            end_id AS station_id,
            end_latitude AS latitude,
            end_longitude AS longitude
        FROM citibike
        WHERE
            end_latitude BETWEEN {TAXI_ZONE_LAT_MIN} AND {TAXI_ZONE_LAT_MAX}
            AND
            end_longitude BETWEEN {TAXI_ZONE_LON_MIN} AND {TAXI_ZONE_LON_MAX}
        GROUP BY 1, 2, 3'''.translate({ord(c): ' ' for c in '\n\t'})
    )

    stations.write.jdbc(
    url = jdbc_url,
    table = 'staging.citibike_stations',
    mode = 'overwrite',
    properties = jdbc_props
)

def citibike_visits():
    visits = spark.sql('''
        SELECT
            month,
            station_id,
            SUM(visits) AS visits
        FROM (
            SELECT
                start_month AS month,
                start_id AS station_id,
                COUNT(*) AS visits
            FROM citibike
            GROUP BY 1, 2
            UNION ALL
            SELECT
                end_month AS month,
                end_id AS station_id,
                COUNT(*) AS visits
            FROM citibike
            GROUP BY 1, 2
        )
        GROUP BY 1, 2
    ''')

    visits.write.jdbc(
        url = jdbc_url,
        table = 'staging.citibike_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )
