import dash
import dash_core_components as dcc
import dash_html_components as html
import json
import os
import pandas as pd
import plotly
import plotly.graph_objects as go
from sqlalchemy import create_engine


#####     SETUP / DATA     #####
token = os.environ['MAPBOX_ACCESS_TOKEN']

py_engine = create_engine(
    'postgresql://' +
    os.environ['PSQL_USER'] + ':' + os.environ['PSQL_PASSWORD'] +
    '@' + os.environ['PSQL_HOST'] + ':' + os.environ['PSQL_PORT'] +
    '/' + os.environ['PSQL_DATABASE']
)

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

yelp_map = go.Choroplethmapbox(
    geojson = json_zones,
    locations = stats['zone_id'].tolist(),
    z = stats['yelp_weighted_sum_reviews'].tolist(),
    text = stats['zone'], 
    # colorbar = dict(thickness = 20, ticklen = 3),
    showscale = False
)

fig = go.Figure()

fig.add_trace(yelp_map)

fig.update_layout(
    autosize = True,
    height = 700,
    mapbox = dict(
        accesstoken = token,
        style = 'dark',
        center = dict(
            lon = -73.9778002135437,
            lat = 40.7058240860865
        ),
        zoom = 9.35
    )
)

stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = stylesheets)

app.layout = html.Div([
    dcc.Graph(figure = fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)



# stations_by_visits = dcc.Graph(
#         id = 'stations_by_visits',
#         figure = {
#             'data': [
#                 dict(
#                     name = zone,
#                     showlegend = False,
#                     x = stats[stats['zone'] == zone]['taxi_visits'],
#                     y = stats[stats['zone'] == zone]['citibike_stations'],
#                     text = stats[stats['zone'] == zone]['zone'],
#                     mode = 'markers'
#                 ) for zone in stats['zone'].unique()
#             ],
#             'layout': dict(
#                 title = {'text': 'Stations by Visits'},
#                 xaxis = {
#                     'title': 'Taxi Visits',
#                     'type': 'linear'
#                 },
#                 yaxis = {
#                     'title': 'Citibike Stations',
#                     'type': 'linear'
#                 },
#                 margin = {'l': 40, 'b': 40, 't': 10, 'r': 10},
#                 legend = {'x': 0, 'y': 1},
#                 hovermode = 'closest'
#             )
#         }
#     )