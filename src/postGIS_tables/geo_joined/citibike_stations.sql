-- Create join table for taxi zones and Citibike stations

DROP TABLE IF EXISTS geo_joined.citibike_stations;

CREATE TABLE geo_joined.citibike_stations AS
	SELECT
		z.zone_id,
		c.station_id
	FROM
		staging.taxi_zones AS z
		JOIN (
			SELECT
				station_id,
				ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry
			FROM staging.citibike_stations
		) AS c
			ON ST_WITHIN(c.geometry, z.geometry)
	GROUP BY 1, 2;
