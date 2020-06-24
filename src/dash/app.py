import json
import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly
import plotly.graph_objects as go
from config.database import py_engine
from config.geometries import TAXI_ZONE_CENTROID_LAT, TAXI_ZONE_CENTROID_LON


token = os.environ['MAPBOX_ACCESS_TOKEN']

zones = pd.read_sql(
    sql = 'taxi_zones_production',
    con = py_engine
)

json_zones = {'type': 'FeatureCollection', 'features': []}
for _, row in zones.iterrows():
    feature = {
        'type':'Feature',
        'id': row['zone_id'],
        'geometry': json.loads(row['geometry'])
    }
    json_zones['features'].append(feature)

stats = pd.read_sql(
    sql = 'all_time_stats_production',
    con = py_engine
)

cols = [
    'taxi_visits',
    'citibike_visits',
    'citibike_stations',
    'yelp_avg_rating',
    'yelp_sum_reviews',
    'yelp_weighted_sum_reviews'
]
map_views = []

for col in cols:
    map_views.append(
        go.Choroplethmapbox(
            geojson = json_zones,
            locations = stats['zone_id'].tolist(),
            z = stats[col].tolist(),
            text = stats['zone_name'] + ', ' + stats['borough'],
            colorbar = dict(thickness=20, ticklen=3),
            # colorscale = 'Hot',
            visible = False
        )
    )

map_views[0]['visible'] = True

fig = go.Figure(data = map_views)

fig.update_layout(
    autosize = True,
    height = 700,
    mapbox = dict(
        accesstoken = token,
        style = 'dark',
        center = dict(
            lon = TAXI_ZONE_CENTROID_LON,
            lat = TAXI_ZONE_CENTROID_LAT
        ),
        zoom = 9.35
    )
)

fig.update_layout(
    updatemenus = [
        dict(
            buttons = list([
                dict(
                    args = [
                        'visible',
                        [True, False, False, False, False, False]
                    ],
                    label = 'Taxi Visits',
                    method = 'restyle'
                ),
                dict(
                    args = [
                        'visible',
                        [False, True, False, False, False, False]
                    ],
                    label = 'Citibike Visits',
                    method = 'restyle'
                ),
                dict(
                    args = [
                        'visible',
                        [False, False, True, False, False, False]
                    ],
                    label = 'Citibike Stations',
                    method = 'restyle'
                ),
                dict(
                    args = [
                        'visible',
                        [False, False, False, True, False, False]
                    ],
                    label = 'Yelp Average Rating',
                    method = 'restyle'
                ),
                dict(
                    args = [
                        'visible',
                        [False, False, False, False, True, False]
                    ],
                    label = 'Yelp Reviews',
                    method = 'restyle'
                ),
                dict(
                    args = [
                        'visible',
                        [False, False, False, False, False, True]
                    ],
                    label = 'Yelp Weighted Reviews',
                    method = 'restyle'
                )
            ]),
        )
    ]
)

stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = stylesheets)

app.layout = html.Div([
    dcc.Graph(figure = fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)
