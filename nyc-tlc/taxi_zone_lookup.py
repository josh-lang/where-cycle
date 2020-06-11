import boto3
from geoalchemy2 import Geometry, WKTElement
import geopandas as gpd
import os
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from sqlalchemy import *
import pandas as pd


# Fetch NYC-TLC's Taxi Zone shape file
s3 = boto3.resource('s3')
s3.meta.client.download_file(
    'nyc-tlc',
    'misc/taxi_zones.zip',
    'taxi_zones.zip'
)

# Convert, filter, and alias the taxi_zones dataframe
taxi_zones = gpd.read_file('zip://taxi_zones.zip') \
    .to_crs('EPSG:4326') \
    .filter(
        [
            'OBJECTID',
            'zone',
            'borough',
            'geometry'
        ],
        axis=1
    ).rename(
        columns={
            'OBJECTID': 'zone_id',
            'zone': 'zone_name'
        }
    )

os.remove('taxi_zones.zip')

# Convert polygons to WKT multipolygons for storage in postGIS
def homogenize(geo):
    multi = MultiPolygon([geo]) if type(geo) == Polygon else geo
    return WKTElement(multi.wkt, srid = 4326)
taxi_zones['geometry'] = taxi_zones['geometry'].apply(homogenize)

# Write taxi_zones table to PostgreSQL
db_url = 'postgresql://' + \
    os.environ['PSQL_USER'] + ':' + os.environ['PSQL_PASSWORD'] + \
    '@' + os.environ['PSQL_HOST'] + ':' + os.environ['PSQL_PORT'] + \
    '/' + os.environ['PSQL_DATABASE']
engine = create_engine(db_url)

taxi_zones.to_sql(
    name = 'taxi_zones',
    con = engine,
    if_exists = 'replace',
    index = False,
    index_label = 'zone_id',
    dtype = {
        'zone_id': Integer(),
        'zone_name': String(length = 45),
        'borough': String(length = 13),
        'geometry': Geometry('MULTIPOLYGON', 4326)
    }
)


# centroid = lookup.filter(['LocationID', 'geometry'], axis=1)
# centroid['latitude'] = centroid['geometry'].centroid.y
# centroid['longitude'] = centroid['geometry'].centroid.x
# centroid = centroid.drop(['geometry'], axis=1)
# centroid = pd.DataFrame(centroid)

# centroid.to_json('../results/taxi_zone_centroids.json', orient='records')
# s3.meta.client.upload_file(
#     '../results/taxi_zone_centroids.json',
#     'jlang-20b-de-ny',
#     'mvp_results/taxi_zone_centroids.json'
# )
