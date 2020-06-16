DROP TABLE IF EXISTS taxi_endpoint_visits;

CREATE TABLE taxi_endpoint_visits AS
	SELECT
		t.month,
		t.zone_id,
		SUM(t.endpoint_visits) AS endpoint_visits
	FROM (
		SELECT
			month,
			zone_id,
			endpoint_visits
		FROM past_taxi_endpoint_visits
		UNION ALL
		SELECT
			month,
			zone_id,
			trips AS endpoint_visits
		FROM modern_taxi_endpoint_visits
	) AS t
	GROUP BY 1, 2
	ORDER BY 1, 2;
