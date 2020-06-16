DROP TABLE IF EXISTS citibike_stations_by_zone;

CREATE TABLE citibike_stations_by_zone AS
	SELECT
		z.zone_id,
		c.station_id
	FROM
		taxi_zones AS z
		JOIN (
			SELECT
				station_id,
				ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry
			FROM citibike_stations_staging
		) AS c
			ON ST_WITHIN(c.geometry, z.geometry)
	GROUP BY 1, 2
	ORDER BY 1, 2;
