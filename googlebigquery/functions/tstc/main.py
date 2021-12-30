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

def get_taxi_coordinates_from_lta():
    '''
    LTA DATAMALL provides up to 500 rows of taxi info,
    so we need to run the API call several times until we have
    aggregated all results. All the taxi coordinates aggregated will
    be returned by this function
    '''
    taxi_coordinates = []
    for index in range(20):
        skip = 0 + 500 * index
        #     print(f'skip at {skip}')
        uri = f'http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability?$skip={skip}'
        headers = {
            'AccountKey': 'BehS/IpVR0KOFQ+BgFqM5g==',
            'accept': 'application/json'
        }  #this is by default
        r = requests.get(url=uri, headers=headers).json()
        if len(requests.get(url=uri, headers=headers).json()["value"]) == 0:
            break
        taxi_coordinates += r['value']
    return taxi_coordinates

def find_nearest_taxi_stand(ts_df,taxi_lat=1.281261, taxi_lon=103.846358):
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
    return df.iloc[:10]


def count_taxis_in_ts(ts_df):
    '''
    First retrieve the taxi coordinates using the LTA API for available taxis

    Then find nearest taxi stand for each taxi and count how many taxi stands
    have how many taxis. Return a dataframe that counts taxis per taxi stands

    This information is the core training data for machine learning models

    Cutoff distance represents how near a taxi to a taxi stand is consider
    inside the taxi stand. Cutoff distance of 0.1 represents 100m = 0.1km
    '''

    ### Older method using GOV.SG API, no longer working
    # taxi_url = "https://api.data.gov.sg/v1/transport/taxi-availability"
    # r = requests.get(taxi_url)
    # coordinates = r.json()["features"][0]["geometry"]["coordinates"]
    # timestamp_str = r.json()['features'][0]['properties']['timestamp']
    # timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S+08:00')

    coordinates=get_taxi_coordinates_from_lta()
    timestamp = datetime.now() + timedelta(hours=8) # Singapore time

    cutoff_distance = 0.200  # Measured in km

    ts_counter = dict(
        zip(ts_df['ts_id'].tolist(), [0 for _ in ts_df['ts_id'].tolist()]))

    for taxi_coordinates in coordinates:
        lon = taxi_coordinates['Longitude']
        lat = taxi_coordinates['Latitude']
        d_df = find_nearest_taxi_stand(ts_df, lat, lon)
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


def gcp_load_df_into_bigquery(df):
    '''
    Write a dataframe into google bigquery
    '''
    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.h_taxi_stand_taxi_count'

    job = client.load_table_from_dataframe(df, table_id)

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows,
                                                       len(table.schema),
                                                       table_id))


# if __name__ == "__main__":
def taxi_stop_taxi_count(request):
    '''
    1. Get taxi stands static
    2. Count taxis in them
    3. Write into big query
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

    ts_df = get_taxi_stands(taxi_stands_json)
    tstc = count_taxis_in_ts(ts_df)
    gcp_load_df_into_bigquery(tstc)

    return ("Done!", 200)


## Requirements
# google-cloud-bigquery==2.31.0
# numpy==1.18.5
# pandas==1.3.1
# requests==2.26.0
# pyarrow==6.0.1
# json
