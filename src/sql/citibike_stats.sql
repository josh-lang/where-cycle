DROP TABLE IF EXISTS citibike_stats;

CREATE TABLE citibike_stats AS
	SELECT
        s.zone_id,
        v.month,
        COUNT(s.station_id) AS stations,
        SUM(v.endpoint_visits) AS endpoint_visits
    FROM
        citibike_stations_by_zone AS s
        JOIN citibike_endpoint_visits AS v
            USING (station_id)
    GROUP BY 1, 2
    ORDER BY 1, 2;
