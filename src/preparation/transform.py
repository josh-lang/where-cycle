import geopandas as gpd
import pandas as pd
from geoalchemy2 import WKTElement
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon


def calculate_centroids(**kwargs):
    '''Calculate centroids for each taxi zone and extract lat-lons'''
    ti = kwargs['ti']
    taxi_zones = ti.xcom_pull(task_ids = 'get_taxi_zones')

    centroids = pd.DataFrame.from_dict({
        'latitude': taxi_zones['geometry'].centroid.y,
        'longitude': taxi_zones['geometry'].centroid.x
    })
    return centroids

def clean_businesses(**kwargs):
    '''
    Drop invalid and duplicated businesses,
    unnest lat-lons, & combine into geometry column
    '''
    ti = kwargs['ti']
    businesses = ti.xcom_pull(task_ids = 'get_businesses')

    businesses.drop(
        businesses[businesses.distance > 3000].index,
        inplace = True
    )
    businesses.sort_values('distance') \
        .drop_duplicates('id', keep ='first') \
        .sort_index()
    businesses.reset_index(
        drop = True,
        inplace = True
    )

    business_coordinates = pd.json_normalize(businesses.coordinates)
    business_coordinates.dropna(how = 'any', inplace = True)

    businesses_flat = businesses.join(business_coordinates, how = 'inner')
    businesses_flat.reset_index(drop = True, inplace = True)

    businesses_geo = gpd.GeoDataFrame(
        businesses_flat,
        geometry = gpd.points_from_xy(
            businesses_flat.longitude,
            businesses_flat.latitude
        )
    )
    businesses_geo['geometry'] = businesses_geo.geometry.apply(
        lambda point: WKTElement(point.wkt, srid = 4326)
    )

    businesses_writable = businesses_geo.filter(
        [
            'id',
            'review_count',
            'rating',
            'geometry'
        ],
        axis = 1
    )
    return businesses_writable

def clean_taxi_zones(**kwargs):
    '''Make geometry column consistent for writing to postgres'''
    ti = kwargs['ti']
    taxi_zones = ti.xcom_pull(task_ids = 'get_taxi_zones')

    def homogenize(geometry):
        '''
        Convert any Polygon to a MultiPolygon
        and then either to a WKTElement
        '''
        multi = MultiPolygon([geometry]) if type(geometry) == Polygon else geometry
        return WKTElement(multi.wkt, srid = 4326)

    taxi_zones['geometry'] = taxi_zones['geometry'].apply(homogenize)
    return taxi_zones
