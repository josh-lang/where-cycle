DROP TABLE IF EXISTS statistics.yelp_businesses;

CREATE TABLE statistics.yelp_businesses AS
    SELECT
        z.zone_id,
        AVG(y.rating) AS avg_rating,
        SUM(y.review_count) AS sum_reviews,
        SUM(y.review_count * y.rating) AS weighted_sum_reviews
    FROM
        staging.taxi_zones AS z
        JOIN staging.yelp_businesses AS y
            ON ST_WITHIN(y.geometry, z.geometry)
    GROUP BY 1;
