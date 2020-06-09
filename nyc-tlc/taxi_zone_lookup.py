import boto3
import geopandas as gpd
import os
import pandas as pd


# user = os.environ['PSQL_USER']
# password = os.environ['PSQL_PASSWORD']
# host = os.environ['PSQL_HOST']
# port = os.environ['PSQL_PORT']
# database = os.environ['PSQL_DATABASE']
# db_string = 'postgresql://' +\
#             user + ':' + password +\
#             '@' + host + ':' + port + '/' + database
# engine = create_engine(db_string)

s3 = boto3.resource('s3')
s3.meta.client.download_file(
    'nyc-tlc',
    'misc/taxi_zones.zip',
    './tmp/taxi_zones.zip'
)

original = gpd.read_file('zip://./tmp/taxi_zones.zip')
converted = original.to_crs('EPSG:4326')
lookup = converted.filter(
    [
        'LocationID',
        'zone',
        'borough',
        'geometry'
    ],
    axis=1
)

centroid = lookup.filter(['LocationID', 'geometry'], axis=1)
centroid['latitude'] = centroid['geometry'].centroid.y
centroid['longitude'] = centroid['geometry'].centroid.x
centroid = centroid.drop(['geometry'], axis=1)
centroid = pd.DataFrame(centroid)

lookup.to_file('../results/taxi_zone_lookup.shp', 'ESRI Shapefile')
s3.meta.client.upload_file(
    '../results/taxi_zone_lookup.shp',
    'jlang-20b-de-ny',
    'mvp_results/taxi_zone_lookup.shp'
)

centroid.to_json('../results/taxi_zone_centroids.json', orient='records')
s3.meta.client.upload_file(
    '../results/taxi_zone_centroids.json',
    'jlang-20b-de-ny',
    'mvp_results/taxi_zone_centroids.json'
)

os.remove('./tmp/taxi_zones.zip')
os.remove('../results/taxi_zone_lookup.cpg')
os.remove('../results/taxi_zone_lookup.dbf')
os.remove('../results/taxi_zone_lookup.prj')
os.remove('../results/taxi_zone_lookup.shx')
