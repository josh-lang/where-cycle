DROP TABLE IF EXISTS yelp_stats;

CREATE TABLE yelp_stats AS
    SELECT
        t.zone_id,
        AVG(y.rating) AS avg_rating,
        SUM(y.review_count) AS sum_reviews,
        SUM(y.review_count * y.rating) AS weighted_sum_reviews
    FROM
        taxi_zones AS t
        JOIN yelp_businesses AS y
            ON ST_WITHIN(y.geometry, t.geometry)
    GROUP BY 1
    ORDER BY 1;
