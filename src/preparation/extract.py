import io
import os
import requests
import time
import zipfile
import boto3
import geopandas as gpd
import pandas as pd


s3 = boto3.resource('s3')

def unzip_csvs():
    '''Iterate over relevant zipped files, unzip, and upload to private s3'''
    source = s3.Bucket('tripdata')

    for obj in source.objects.all():
        key = obj.key

        if not key.startswith('201307-201402') and key.endswith('.zip'):
            buffer = io.BytesIO(obj.get()['Body'].read())
            zipped = zipfile.ZipFile(buffer)

            for name in zipped.namelist():

                if not name.startswith('_') and name.endswith('.csv'):
                    s3.meta.client.upload_fileobj(
                        zipped.open(name),
                        Bucket = 'jlang-20b-de-ny',
                        Key = 'citibike/' + name
                    )

def get_taxi_zones():
    '''Pull taxi zone shapfile and convert to EPSG 4326'''
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

def get_businesses(**kwargs):
    '''For each taxi zone, query Yelp API for businesses closest to centroid'''
    ti = kwargs['ti']
    centroids = ti.xcom_pull(task_ids = 'calculate_centroids')
    
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
