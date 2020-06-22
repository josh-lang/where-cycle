import os
import requests
import time
import boto3
import geopandas as gpd
import pandas as pd


def get_taxi_zones():
    '''Pull taxi zone shapfile and convert to EPSG 4326'''
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
            axis = 1
        ).rename(
            columns = {
                'OBJECTID': 'zone_id',
                'zone': 'zone_name'
            }
        )
    os.remove('taxi_zones.zip')
    return taxi_zones

def get_businesses(centroids):
    '''For each taxi zone, query Yelp API for businesses closest to centroid'''
    api_key = 'Bearer ' + os.environ['YELP_API_KEY']
    head = {'Authorization': api_key}
    url = 'https://api.yelp.com/v3/businesses/search'
    businesses = pd.DataFrame()

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
            time.sleep(1)
            response = requests.get(url, headers = head, params = query)
            json = response.json()
        matches = json['businesses']
        businesses = businesses.append(matches, ignore_index = True)
    return businesses
