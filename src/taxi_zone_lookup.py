import boto3
from geoalchemy2 import Geometry, WKTElement
import geopandas as gpd
import os
import pandas as pd
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from sqlalchemy import Float, Integer, String
import requests
from time import sleep
from db_config import py_engine

s3 = boto3.resource('s3')
s3.meta.client.download_file(
    'nyc-tlc',
    'misc/taxi_zones.zip',
    'taxi_zones.zip'
)

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

api_key = 'Bearer ' + os.environ['YELP_API_KEY']
head = {'Authorization': api_key}
url = 'https://api.yelp.com/v3/businesses/search'

centroids = pd.DataFrame.from_dict({
    'latitude': taxi_zones['geometry'].centroid.y,
    'longitude': taxi_zones['geometry'].centroid.x
})

yelp = pd.DataFrame()
for _, row in centroids.iterrows():
    query = {
        'latitude': row['latitude'],
        'longitude': row['longitude'],
        'radius': 3000,
        'limit': 50,
        'sort_by': 'distance'
    }

    response = requests.get(url, headers = head, params = query)
    json = response.json()
    while 'error' in json:
        sleep(1)
        response = requests.get(url, headers = head, params = query)
        json = response.json()
    matches = json['businesses']
    yelp = yelp.append(matches, ignore_index = True)

yelp.drop(
    yelp[yelp.distance > 3000].index,
    inplace = True
)

yelp.sort_values('distance') \
    .drop_duplicates('id', keep='first') \
    .sort_index()

yelp.reset_index(
    drop = True,
    inplace = True
)

yelp_coordinates = pd.json_normalize(yelp.coordinates)
yelp_coordinates.dropna(how = 'any', inplace = True)

yelp_flat = yelp.join(yelp_coordinates, how = 'inner')
yelp_flat.reset_index(drop = True, inplace = True)

yelp_geo = gpd.GeoDataFrame(
    yelp_flat,
    geometry = gpd.points_from_xy(
        yelp_flat.longitude,
        yelp_flat.latitude
    )
)

def homogenize(geo):
    multi = MultiPolygon([geo]) if type(geo) == Polygon else geo
    return WKTElement(multi.wkt, srid = 4326)
taxi_zones['geometry'] = taxi_zones['geometry'].apply(homogenize)

taxi_zones.to_sql(
    name = 'taxi_zones',
    con = py_engine,
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

yelp_geo['geometry'] = yelp_geo.geometry.apply(
    lambda point: WKTElement(point.wkt, srid = 4326)
)

yelp_writable = yelp_geo.filter(
    [
        'id',
        'review_count',
        'rating',
        'geometry'
    ],
    axis = 1
)

yelp_writable.to_sql(
    name = 'yelp_businesses',
    con = py_engine,
    if_exists = 'replace',
    index = False,
    index_label = 'business_id',
    dtype = {
        'business_id': String(22),
        'review_count': Integer(),
        'rating': Float(),
        'geometry': Geometry('POINT', 4326)
    }
)
