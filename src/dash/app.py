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

zones = pd.read_sql_table(
    table_name = 'taxi_zones',
    con = py_engine,
    schema = 'production'
)

json_zones = {'type': 'FeatureCollection', 'features': []}
for _, row in zones.iterrows():
    feature = {
        'type':'Feature',
        'id': row['zone_id'],
        'geometry': json.loads(row['geometry'])
    }
    json_zones['features'].append(feature)

stats = pd.read_sql_table(
    table_name = 'all_time_stats',
    con = py_engine,
    schema = 'production'
)

columns = [
    'tlc_visits',
    'citibike_visits',
    'citibike_stations',
    'yelp_avg_rating',
    'yelp_sum_reviews',
    'yelp_weighted_sum_reviews'
]

map_views = []
bar_charts = []

for column in columns:
    map_views.append(
        go.Choroplethmapbox(
            geojson = json_zones,
            locations = stats['zone_id'].tolist(),
            z = stats[column].tolist(),
            text = stats['zone_name'] + ', ' + stats['borough'],
            visible = False,
            subplot = 'mapbox',
            hovertemplate = '%{text}<br />' +
                            '%{z}<br />' +
                            '<extra></extra>'
        )
    )

    top = stats.sort_values([column], ascending = False).head(15)
    bar_charts.append(
        go.Bar(
            x = top[column],
            y = top['zone_name'] + ', ' + top['borough'],
            text = top['zone_name'] + ', ' + top['borough'],
            textposition = 'inside',
            hovertemplate = '%{text}<br />' +
                            '%{x}<br />' +
                            '<extra></extra>',
            xaxis = 'x',
            yaxis = 'y',
            marker = dict(color = top[column]),
            visible = False,
            name = '',
            orientation = 'h'
        )
    )

map_views[0]['visible'] = True
bar_charts[0]['visible'] = True

fig = go.Figure(data = map_views + bar_charts)

fig.update_layout(
    title = dict(
        text = 'Where Cycle',
        font = {'size': 36},
        x = 0.5,
        xanchor = 'center'
    ),
    autosize = True,
    height = 700,
    mapbox = dict(
        domain = {'x': [0.25, 1], 'y': [0, 1]},
        accesstoken = token,
        style = 'dark',
        center = dict(
            lon = TAXI_ZONE_CENTROID_LON,
            lat = TAXI_ZONE_CENTROID_LAT
        ),
        zoom = 9.35
    ),
    xaxis = {
        'domain': [0, 0.25],
        'anchor': 'x',
        'showticklabels': True,
        'showgrid': True
    },
    yaxis = {
        'domain': [0, 1],
        'anchor': 'y',
        'autorange': 'reversed',
        'visible': False
    },
    margin = dict(l = 0, r = 0, t = 70, b = 50)
)

fig.update_layout(
    updatemenus = [dict(
        x = 0,
        y = 1,
        xanchor = 'left',
        yanchor = 'bottom',
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
    )]
)

stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = stylesheets)

app.layout = html.Div([
    dcc.Location(id = 'url', pathname = '/where-cycle', refresh = False),
    dcc.Graph(figure = fig),
    html.Div([
        'Read more about this project on ',
        html.A(
            ['Github'],
            href = 'https://github.com/josh-lang/where-cycle'
        )
    ])
])

if __name__ == '__main__':
    app.run_server(debug=True)
