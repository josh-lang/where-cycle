DROP TABLE IF EXISTS taxi_zones_production;

CREATE TABLE taxi_zones_production AS
    SELECT
		zone_id,
        ST_ASGeoJSON(ST_ForcePolygonCW(geometry)) AS geometry
    FROM taxi_zones
    ORDER BY 1;
