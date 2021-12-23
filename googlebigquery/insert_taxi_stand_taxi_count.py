import json
import pandas as pd
import numpy as np
import requests
from datetime import datetime
from google.cloud import bigquery


with open('../raw_data/lta-taxi-stop-geojson.geojson') as geofile:
    '''
    Take geojson file provided by LTA where the taxi stands coordinates are provided
    '''
    taxi_stands_json = json.load(geofile)


def get_taxi_stands():
    '''
    Parse the geojson with taxi stands into a set of taxi stand id and lat,lon
    This information is static, so no timestamp is required

    Run this function once. We could store this data frame and not run
    this function every time, but for educational purposes I'm keeping all here
    so is easier to understand where things come from
    '''
    taxi_stands_dict = []
    for ts in taxi_stands_json['features']:
        taxi_stands_dict.append({
            'ts_id': ts['properties']['Name'],
            'lat': ts['geometry']['coordinates'][1],
            'lon': ts['geometry']['coordinates'][0]
        })
    ts_df = pd.DataFrame(taxi_stands_dict)
    return ts_df


def find_nearest_taxi_stand(taxi_lat=1.281261, taxi_lon=103.846358):
    '''
    Given all the static positions of the nearby taxi stands
    we can get the distance with all of them, and return the nearest 10 taxi stands.

    We can assume that a taxi is IN the taxi stand when the distance is < 100 m = 0.1 km

    Python wise we'll be using broadcasting method, so we do the difference of all taxi stands (ts)
    with a given taxi_lat and taxi_lon in one shot, not in a for loop.
    '''

    taxi_lat_rad = np.deg2rad(taxi_lat)
    taxi_lon_rad = np.deg2rad(taxi_lon)

    ts_lat = np.array(ts_df['lat'].tolist())
    ts_lat_rad = np.deg2rad(ts_lat)

    ts_lon = np.array(ts_df['lon'].tolist())
    ts_lon_rad = np.deg2rad(ts_lon)

    dlat = ts_lat_rad - taxi_lat_rad
    dlon = ts_lon_rad - taxi_lon_rad

    d = np.sin(
        dlat /
        2)**2 + np.cos(ts_lat_rad) * np.cos(taxi_lat_rad) * np.sin(dlon / 2)**2
    distance = 2 * 6371 * np.arcsin(np.sqrt(d))

    df = ts_df.copy()
    df['distance'] = distance
    df.sort_values(by='distance', inplace=True)
    return df.iloc[:10]


def count_taxis_in_ts():
    '''
    First retrieve the taxi coordinates using the LTA API for available taxis

    Then find nearest taxi stand for each taxi and count how many taxi stands
    have how many taxis. Return a dataframe that counts taxis per taxi stands

    This information is the core training data for machine learning models
    '''
    taxi_url = "https://api.data.gov.sg/v1/transport/taxi-availability"
    r = requests.get(taxi_url)
    coordinates = r.json()["features"][0]["geometry"]["coordinates"]
    timestamp_str = r.json()['features'][0]['properties']['timestamp']
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S+08:00')

    cutoff_distance = 0.1

    ts_counter = dict(
        zip(ts_df['ts_id'].tolist(), [0 for _ in ts_df['ts_id'].tolist()]))

    for taxi_coordinates in coordinates:
        lon, lat = taxi_coordinates
        d_df = find_nearest_taxi_stand(lat, lon)
        d_df = d_df[d_df['distance'] < cutoff_distance]
        for ts in d_df['ts_id'].tolist():
            ts_counter[ts] += 1

    tmp_taxi_stand_counter = pd.DataFrame.from_dict(ts_counter, orient='index')
    tmp_taxi_stand_counter.reset_index(inplace=True)
    tmp_taxi_stand_counter['timestamp'] = timestamp
    tmp_taxi_stand_counter = tmp_taxi_stand_counter.rename(columns={
        0: 'taxi_count',
        'index': 'ts_id'
    })
    # tmp_taxi_stand_counter
    return ts_df.merge(tmp_taxi_stand_counter)

def gcp_load_df_into_bigquery():
    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.h_taxi_stand_taxi_count'

    job = client.load_table_from_dataframe(df, table_id)

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows,
                                                       len(table.schema),
                                                       table_id))

if __name__ == "__main__":
