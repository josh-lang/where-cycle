DROP TABLE IF EXISTS past_taxi_endpoint_visits;

CREATE TABLE past_taxi_endpoint_visits AS
	SELECT
		p.month,
		z.zone_id,
		SUM(p.endpoint_visits) AS endpoint_visits
	FROM
		taxi_zones AS z
		JOIN (
			SELECT
				month,
				ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry,
				endpoint_visits
			FROM past_taxi_endpoint_visits_staging
		) AS p
			ON ST_WITHIN(p.geometry, z.geometry)
	GROUP BY 1, 2
	ORDER BY 1, 2;
