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

def past_tlc_visits():
    visits = spark.sql(f'''
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
                pickup_longitude BETWEEN {TAXI_ZONE_LON_MIN} AND {TAXI_ZONE_LON_MAX}
                AND
                pickup_latitude BETWEEN {TAXI_ZONE_LAT_MIN} AND {TAXI_ZONE_LAT_MAX}
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT
                month,
                ROUND(dropoff_longitude, 3) AS longitude,
                ROUND(dropoff_latitude, 3) AS latitude,
                COUNT(*) AS visits
            FROM past
            WHERE
                dropoff_longitude BETWEEN {TAXI_ZONE_LON_MIN} AND {TAXI_ZONE_LON_MAX}
                AND
                dropoff_latitude BETWEEN {TAXI_ZONE_LAT_MIN} AND {TAXI_ZONE_LAT_MAX}
            GROUP BY 1, 2, 3
        )
        GROUP BY 1, 2, 3'''.translate({ord(c): ' ' for c in '\n\t'})
    )

    visits.write.jdbc(
        url = jdbc_url,
        table = 'staging.past_tlc_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )

def modern_tlc_visits():
    modern = spark.sql('''
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

    modern.write.jdbc(
        url = jdbc_url,
        table = 'staging.modern_tlc_visits',
        mode = 'overwrite',
        properties = jdbc_props
    )
