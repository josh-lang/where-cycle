import pandas
from geoalchemy2 import Geometry
from sqlalchemy import Float, Integer, String
from config.database import py_engine


def write_businesses(**kwargs):
    '''Write Yelp business data to postgres for further processing'''
    ti = kwargs['ti']
    businesses = ti.xcom_pull(task_ids = 'clean_businesses')

    businesses.to_sql(
        name = 'yelp_businesses',
        con = py_engine,
        schema = 'staging',
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

def write_taxi_zones(**kwargs):
    '''Write taxi zone map to postgres'''
    ti = kwargs['ti']
    taxi_zones = ti.xcom_pull(task_ids = 'clean_taxi_zones')

    taxi_zones.to_sql(
        name = 'taxi_zones',
        con = py_engine,
        schema = 'staging',
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
