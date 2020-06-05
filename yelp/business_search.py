import os
import pandas as pd
import requests


api_key = os.environ['YELP_API_KEY']
base_url = 'https://api.yelp.com/v3/businesses/search'
client_id = os.environ['YELP_CLIENT_ID']
centroids = pd.read_json('../results/taxi_zone_centroids.json', orient='records')
