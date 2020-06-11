import boto3
import os
import pandas as pd
import requests


api_key = 'Bearer ' + os.environ['YELP_API_KEY']
head = {'Authorization': api_key}
centroids = pd.read_json('../results/taxi_zone_centroids.json', orient='records')
url = 'https://api.yelp.com/v3/businesses/search'

businesses = pd.DataFrame()
for index, row in centroids.iterrows():
    query = {
        'latitude': row['latitude'],
        'longitude': row['longitude'],
        'radius': 3000,
        'limit': 50,
        'sort_by': 'distance'
    }

    res = requests.get(url, headers=head, params=query)
    matches = res.json()['businesses']
    businesses = businesses.append(matches, ignore_index=True)

businesses = businesses \
    .sort_values('distance') \
    .drop_duplicates('id', keep='first') \
    .sort_index() \
    .filter(
        [
            'id',
            'review_count',
            'rating',
            'coordinates',
            'distance'
        ],
        axis=1
    )

s3 = boto3.resource('s3')
businesses.to_json('../results/yelp_businesses.json', orient='records')
s3.meta.client.upload_file(
    '../results/yelp_businesses.json',
    'jlang-20b-de-ny',
    'mvp_results/yelp_businesses.json'
)
