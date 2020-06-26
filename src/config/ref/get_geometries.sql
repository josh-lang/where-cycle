SELECT ST_Extent(geometry) AS bbox
FROM staging.taxi_zones;

SELECT ST_AsText(ST_Centroid(ST_Extent(geometry))) AS center
FROM staging.taxi_zones;
