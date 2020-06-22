DROP TABLE IF EXISTS all_time_stats_production;

CREATE TABLE all_time_stats_production AS
    SELECT
        v.zone_id,
        v.zone_name,
        v.borough,
        v.taxi_visits,
        v.citibike_visits,
        v.citibike_stations,
        y.avg_rating AS yelp_avg_rating,
        y.sum_reviews AS yelp_sum_reviews,
        y.weighted_sum_reviews AS yelp_weighted_sum_reviews
    FROM
        (
            SELECT
                z.zone_id,
                z.zone_name,
                z.borough,
                COALESCE(SUM(t.endpoint_visits), 0) AS taxi_visits,
                COALESCE(SUM(c.visits), 0) AS citibike_visits,
                COALESCE(MAX(c.stations), 0) AS citibike_stations
            FROM
                taxi_zones AS z
                LEFT JOIN taxi_endpoint_visits AS t USING (zone_id)
                LEFT JOIN citibike_stats AS c
                    ON t.zone_id = c.zone_id AND t.month = c.month
            GROUP BY 1, 2, 3
        ) AS v
        LEFT JOIN yelp_stats AS y USING (zone_id)
    ORDER BY 1;
