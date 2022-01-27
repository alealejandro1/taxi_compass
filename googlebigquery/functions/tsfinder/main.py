import json
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

def get_taxi_stands(taxi_stands_json):
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


def find_nearest_taxi_stand(ts_df,
                            taxi_lat=1.281261,
                            taxi_lon=103.846358,
                            taxi_length=5):
    '''
    Given all the static positions of the nearby taxi stands
    we can get the distance with all of them, and return the nearest 10 taxi stands.

    We can assume that a taxi is IN the taxi stand when the distance is < 100 m = 0.1 km

    Python wise we'll be using broadcasting method, so we do the difference of all taxi stands (ts)
    with a given taxi_lat and taxi_lon in one shot, not in a for loop.
    '''
    taxi_lat = np.float64(taxi_lat)
    taxi_lon = np.float64(taxi_lon)
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
    return df[['ts_id']].iloc[:taxi_length].values.flatten().tolist()


# if __name__ == "__main__":
def taxi_stop_finder(request):
    '''
    1. Get taxi stands static
    2. Return nearby taxi stands
    '''
    ### GCP Storage - - - - - - - - - - - - - - - - - - - - - -
    BUCKET_NAME = 'static-file-storage'
    BUCKET_TAXI_STAND_GEOJSON_PATH = 'lta-taxi-stop-geojson.geojson'

    # Add Client() here
    storage_client = storage.Client()
    path = f"gs://{BUCKET_NAME}/{BUCKET_TAXI_STAND_GEOJSON_PATH}"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BUCKET_TAXI_STAND_GEOJSON_PATH)

    with blob.open('r') as geofile:
        # with open(public_path) as geofile:
        '''
        Take geojson file provided by LTA where the taxi stands coordinates are provided
        '''
        taxi_stands_json = json.load(geofile)
        print('loaded json successfully from bucket')

    # Here comes the request info with the taxi lat lon
    request_json = request.get_json()
    taxi_lat=request_json['latitude']
    taxi_lon=request_json['longitude']
    taxi_length = request_json['length']
    ts_df = get_taxi_stands(taxi_stands_json)
    nearby_taxi_stands = find_nearest_taxi_stand(ts_df, taxi_lat, taxi_lon,
                                                 taxi_length)

    return ('-'.join(nearby_taxi_stands), 200)


## Requirements
# google-cloud-bigquery==2.31.0
# numpy==1.18.5
# pandas==1.3.1
# requests==2.26.0
# pyarrow==6.0.1
# json

# Testing: {"latitude":1.281260 , "longitude":103.8443}
