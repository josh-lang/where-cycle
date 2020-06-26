-- Combine past TLC visits with modern TLC visits
-- and aggregate by taxi zone ID

DROP TABLE IF EXISTS statistics.tlc_visits;

CREATE TABLE statistics.tlc_visits AS
	SELECT
		t.month,
		t.zone_id,
		SUM(t.visits) AS visits
	FROM (
		SELECT
			month,
			zone_id,
			visits
		FROM geo_joined.past_tlc_visits
		UNION ALL
		SELECT
			month,
			zone_id,
			visits
		FROM staging.modern_tlc_visits
	) AS t
	GROUP BY 1, 2;
