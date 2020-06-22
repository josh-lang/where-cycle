import pandas
from geoalchemy2 import Geometry
from sqlalchemy import Float, Integer, String
from config.database import py_engine


def write_businesses(businesses):
    '''Write Yelp business data to postgres for further processing'''
    businesses.to_sql(
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

def write_taxi_zones(zones):
    '''Write taxi zone map to postgres'''
    zones.to_sql(
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
