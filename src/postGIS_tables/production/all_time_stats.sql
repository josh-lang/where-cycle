DROP TABLE IF EXISTS production.all_time_stats;

CREATE TABLE production.all_time_stats AS
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
                COALESCE(SUM(t.visits), 0) AS tlc_visits,
                COALESCE(SUM(c.visits), 0) AS citibike_visits,
                COALESCE(MAX(c.stations), 0) AS citibike_stations
            FROM
                staging.taxi_zones AS z
                LEFT JOIN statistics.tlc_visits AS t USING (zone_id)
                LEFT JOIN statistics.citibike AS c
                    ON t.zone_id = c.zone_id AND t.month = c.month
            GROUP BY 1, 2, 3
        ) AS v
        LEFT JOIN statistics.yelp_businesses AS y USING (zone_id)
    ORDER BY 1;
