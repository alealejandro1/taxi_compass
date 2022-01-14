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

def get_coordinates():
    '''
    Get latitude and longitude based on your ISP substation location.
    '''
    g = geocoder.ip('me')
    coordinates = g.latlng
    return coordinates

def SQL_Query(taxi_stands_string):
    '''
    Takes a taxi_stand_list and performs a query on the latest
    predictions on taxi_stand occupation
    '''
    # This info should be kept in params. Credentials not needed when running
    # from within the GCP
    taxi_stand_tuple = tuple(taxi_stands_string.split('-'))
    #bq_key_path = '/Users/alejandroseif/Documents/GCP/BigQuerykey/taxi-compass-lewagon-0548ea55c10c.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = bq_key_path
    bigquery_client = bigquery.Client(project='taxi-compass-lewagon')

    QUERY_TS_LIST = f"""
    SELECT timestamp, ts_id, lat, lon, taxi_count
    FROM `taxi-compass-lewagon.api_dataset.h_taxi_stand_taxi_count`
    WHERE ts_id in {taxi_stand_tuple}
    ORDER BY timestamp DESC
    LIMIT {len(taxi_stand_tuple)}
    """
    query_job = bigquery_client.query(QUERY_TS_LIST)
    query_df = query_job.to_dataframe()
    return query_df

def color_guide(count):
    if count == 0:
        return 'green'
    if count >= 1 and count < 3:
        return 'darkgreen'
    if count >= 3 and count < 6:
        return 'orange'
    else:
        return 'black'

st.markdown("""# Taxi Compass
## Want to find out what the taxi count is in nearby taxi stands?""")
if "coordinates" not in st.session_state:
    st.session_state.coordinates = ()


if st.button('Press to retrieve your coordinates'):
    # print is visible in the server output, not in the page
    st.session_state.coordinates = get_coordinates()
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    st.write(f'Coordinates obtained @ {current_time}! \
        Lat:{st.session_state.coordinates[0]} Long:{st.session_state.coordinates[1]}'
             )
    m = folium.Map(location=[
        st.session_state.coordinates[0], st.session_state.coordinates[1]
    ],
                   zoom_start=13)

    folium.Marker(
        location=[
            st.session_state.coordinates[0], st.session_state.coordinates[1]
        ],
        popup='You are here',
        icon=folium.Icon(color="darkblue", icon="car"),
    ).add_to(m)
    folium_static(m)

if st.button('Press to Retrieve Nearby Taxi Count Predictions'):
    # Use Lat Long to retrieve nearby Taxi Stands in a taxi_stand_tuple
    # SQL query from prediction table, filter by Nearby Taxi Stands
    st.write(f'The following are your nearby taxi stands, their \
                current and predicted taxi count in 15min'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               )
    ## First get nearby taxi stands using the cloud function tsfinder:
    r = requests.post(
        'https://us-central1-taxi-compass-lewagon.cloudfunctions.net/tsfinder',
        json={
            "latitude": st.session_state.coordinates[0],
            "longitude": st.session_state.coordinates[1]
        })
    ## Pass the list of 10 nearby taxistands to perform the SQL Query
    results_df = SQL_Query(r.text)
    #st.write(results_df[['ts_id','taxi_count']])
    m = folium.Map(location=[
        st.session_state.coordinates[0], st.session_state.coordinates[1]
    ],
                   zoom_start=13)
    folium.Marker(
        location=[
            st.session_state.coordinates[0], st.session_state.coordinates[1]
        ],
        popup='You are here',
        icon=folium.Icon(color="darkblue", icon="car"),
    ).add_to(m)

    for index,row in results_df.iterrows():
        folium.Marker(
            location=[row.lat, row.lon],
            popup=f'There are {row.taxi_count} taxis here',
            icon=folium.Icon(color=color_guide(row.taxi_count), icon="car"),
        ).add_to(m)
    folium_static(m)
