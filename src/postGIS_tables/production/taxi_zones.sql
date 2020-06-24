DROP TABLE IF EXISTS production.taxi_zones;

CREATE TABLE production.taxi_zones AS
    SELECT
		zone_id,
        ST_ASGeoJSON(ST_ForcePolygonCW(geometry)) AS geometry
    FROM staging.taxi_zones
    ORDER BY 1;
