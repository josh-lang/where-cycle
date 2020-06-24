DROP TABLE IF EXISTS geo_joined.past_tlc_visits;

CREATE TABLE geo_joined.past_tlc_visits AS
	SELECT
		p.month,
		z.zone_id,
		SUM(p.visits) AS visits
	FROM
		staging.taxi_zones AS z
		JOIN (
			SELECT
				month,
				ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geometry,
				visits
			FROM staging.past_tlc_visits
		) AS p
			ON ST_WITHIN(p.geometry, z.geometry)
	GROUP BY 1, 2;
