DROP TABLE IF EXISTS citibike_stats;

CREATE TABLE citibike_stats AS
    SELECT
        t.month,
        t.zone_id,
        MAX(active_stations) OVER (
            PARTITION BY t.zone_id
            ORDER BY t.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS stations,
        endpoint_visits
    FROM (
        SELECT
            v.month,
            s.zone_id,
            COUNT(s.station_id) AS active_stations,
            SUM(v.endpoint_visits) AS endpoint_visits
        FROM
            citibike_stations_by_zone AS s
            JOIN citibike_endpoint_visits AS v
                USING (station_id)
        GROUP BY 1, 2
    ) AS t
    ORDER BY 1, 2;
