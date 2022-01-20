import streamlit as st
import numpy as np
import pandas as pd
import geocoder
from datetime import datetime
import os
from google.cloud import bigquery
from streamlit_folium import folium_static
import folium
import requests
from bokeh.models.widgets import Button
from bokeh.models import CustomJS
from streamlit_bokeh_events import streamlit_bokeh_events
from branca.element import Template, MacroElement


def SQL_prediction_date():
    '''
    Upon starting, take the distinct available dates for prediction
    '''
    QUERY_PREDICTION_TIME = f"""
    Select distinct(datetime(timestamp(timestamp_pred,"Asia/Singapore"), "Asia/Singapore")) as pred_dates, timestamp_pred
    from `taxi-compass-lewagon.api_dataset.r_taxi_stand_pred`
    where timestamp(timestamp_pred,"Asia/Singapore") > current_timestamp()
    order by pred_dates asc
    """
    query_job = bigquery_client.query(QUERY_PREDICTION_TIME)
    query_df = query_job.to_dataframe()
    return query_df

def SQL_Query(taxi_stands_string):
    '''
    Takes a taxi_stand_list and performs a query on the latest
    predictions on taxi_stand occupation
    '''
    taxi_stand_tuple = tuple(taxi_stands_string.split('-'))

    QUERY_TS_LIST = f"""
    SELECT timestamp, ts_id, lat, lon, taxi_count
    FROM `taxi-compass-lewagon.api_dataset.h_taxi_stand_taxi_count`
    WHERE ts_id in {taxi_stand_tuple}
    ORDER BY timestamp DESC
    LIMIT {len(taxi_stand_tuple)}
    """

    QUERY_TS_PRED =f"""
    SELECT p.taxi_st_id as ts_id, p.taxi_count_pred as prediction, p.timestamp_pred as timestamp_pred,
    c.taxi_st_lat as latitude, c.taxi_st_lon as longitude
    FROM `taxi-compass-lewagon.api_dataset.r_taxi_stand_pred` as p
    LEFT JOIN `taxi-compass-lewagon.api_dataset.c_taxi_stand` as c
    ON p.taxi_st_id = c.taxi_st_id
    WHERE timestamp_pred = {time_df} AND p.taxi_st_id in {taxi_stand_tuple}
    """

    query_job = bigquery_client.query(QUERY_TS_PRED)
    query_df = query_job.to_dataframe()
    return query_df

def color_guide(count):
    colors = {
        0: 'lightgreen',
        1: 'green',
        2: 'green',
        3: 'blue',
        4: 'blue'
    }
    if count > 4:
        return 'black'
    else:
        return colors[count]

# Big Query setup
# This info should be kept in params. Credentials not needed when running
# from within the GCP
bq_key_path = 'google-credentials.json'  ## Env variable in Heroku
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = bq_key_path
bigquery_client = bigquery.Client(project='taxi-compass-lewagon')

st.markdown("""# Taxi Compass
## What is the taxi count in nearby taxi stands?""")
if "coordinates" not in st.session_state:
    st.session_state.coordinates = ()

prediction_date_df = SQL_prediction_date()

### Radio Button for search range
time_range_df = SQL_prediction_date()
time_df = st.selectbox('Select what time in the future you want to predict', time_range_df)
# length_range = pd.DataFrame({'first column': list([5, 10, 15])})
# taxi_length = st.selectbox('Select how many taxi stands you want to see',length_range)
taxi_length = 20
st.write(f'You will be getting the nearest {taxi_length} taxi stops at {time_df}')
###

loc_button = Button(label="Using Location Get Taxis Near Me", button_type="danger")
loc_button.js_on_event(
    "button_click",
    CustomJS(code="""
    navigator.geolocation.getCurrentPosition(
        (loc) => {
            document.dispatchEvent(new CustomEvent("GET_LOCATION", {detail: {lat: loc.coords.latitude, lon: loc.coords.longitude}}))
        }
    )
    """))
result = streamlit_bokeh_events(loc_button,
                                events="GET_LOCATION",
                                key="get_location",
                                refresh_on_update=False,
                                override_height=75,
                                debounce_time=0)

if result:
    if "GET_LOCATION" in result:
        coordinates = result.get("GET_LOCATION")
        st.write(f'Location obtained!')
        st.session_state.coordinates = [coordinates['lat'],coordinates['lon']]


        # Use Lat Long to retrieve nearby Taxi Stands in a taxi_stand_tuple
        # SQL query from prediction table, filter by Nearby Taxi Stands

        st.write(f'The following are your nearby taxi stands \
                    predicted taxi count'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 )
        ## First get nearby taxi stands using the cloud function tsfinder:
        ## Amount of taxi stands returned is computed on tsfinder cloud function
        ## using taxi_length parameter in POST
        r = requests.post(
            'https://us-central1-taxi-compass-lewagon.cloudfunctions.net/tsfinder',
            json={
                "latitude": st.session_state.coordinates[0],
                "longitude": st.session_state.coordinates[1],
                "length": taxi_length
            })
        ## Pass the list of $taxi_length nearby taxistands to perform the SQL Query
        results_df = SQL_Query(r.text)
        # results_df = pd.DataFrame({})

        m = folium.Map(location=[
            st.session_state.coordinates[0], st.session_state.coordinates[1]
        ],
                       zoom_start=14,
                       tiles='openstreetmap')

        folium.Marker(
            location=[
                st.session_state.coordinates[0],
                st.session_state.coordinates[1]
            ],
            popup='You are here',
            icon=folium.Icon(color="red", icon="car", prefix='fa'),
        ).add_to(m)



        for index,row in results_df.iterrows():
            folium.Marker(
                location=[row.latitude, row.longitude],
                popup=f'Predicted Taxi Count here: {row.prediction}',
                icon=folium.Icon(color=color_guide(row.prediction),
                                 icon="car"),
            ).add_to(m)

            # folium.CircleMarker(location=[row.latitude, row.longitude],
            #                     radius=15,
            #                     color=color_guide(row.prediction),
            #                     stroke=True,
            #                     weight=30,
            #                     opacity=0.1 + 0.2 * row.prediction).add_to(m)


        ####
        template = """
{% macro html(this, kwargs) %}

<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>jQuery UI Draggable - Default functionality</title>
  <link rel="stylesheet" href="//code.jquery.com/ui/1.12.1/themes/base/jquery-ui.css">

  <script src="https://code.jquery.com/jquery-1.12.4.js"></script>
  <script src="https://code.jquery.com/ui/1.12.1/jquery-ui.js"></script>

  <script>
  $( function() {
    $( "#maplegend" ).draggable({
                    start: function (event, ui) {
                        $(this).css({
                            right: "auto",
                            top: "auto",
                            bottom: "auto"
                        });
                    }
                });
});

  </script>
</head>
<body>


<div id='maplegend' class='maplegend'
    style='position: absolute; z-index:9999; border:2px solid grey; background-color:rgba(255, 255, 255, 0.8);
     border-radius:6px; padding: 10px; font-size:14px; right: 20px; bottom: 20px;'>

<div class='legend-title'>Legend</div>
<div class='legend-scale'>
  <ul class='legend-labels'>
    <li><span style='background:red;opacity:0.7;'></span>You</li>
    <li><span style='background:lightgreen;opacity:0.7;'></span>No Taxis</li>
    <li><span style='background:green;opacity:0.7;'></span>Few Taxis</li>
    <li><span style='background:blue;opacity:0.7;'></span>Several Taxis</li>
    <li><span style='background:black;opacity:0.7;'></span>Lots of Taxis</li>

  </ul>
</div>
</div>

</body>
</html>

<style type='text/css'>
  .maplegend .legend-title {
    text-align: left;
    margin-bottom: 5px;
    font-weight: bold;
    font-size: 90%;
    }
  .maplegend .legend-scale ul {
    margin: 0;
    margin-bottom: 5px;
    padding: 0;
    float: left;
    list-style: none;
    }
  .maplegend .legend-scale ul li {
    font-size: 80%;
    list-style: none;
    margin-left: 0;
    line-height: 18px;
    margin-bottom: 2px;
    }
  .maplegend ul.legend-labels li span {
    display: block;
    float: left;
    height: 16px;
    width: 30px;
    margin-right: 5px;
    margin-left: 0;
    border: 1px solid #999;
    }
  .maplegend .legend-source {
    font-size: 80%;
    color: #777;
    clear: both;
    }
  .maplegend a {
    color: #777;
    }
</style>
{% endmacro %}"""

        macro = MacroElement()
        macro._template = Template(template)

        # m.get_root().add_child(macro)
        macro.add_to(m)

        ###

        folium_static(m)
