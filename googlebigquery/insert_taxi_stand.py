from google.cloud import bigquery
import pandas as pd
import geopandas as gpd
import requests
import re

base_url = "https://developers.onemap.sg/privateapi/popapi"
ONEMAP_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjgyNjMsInVzZXJfaWQiOjgyNjMsImVtYWlsIjoiam9obml0bGVlQGdtYWlsLmNvbSIsImZvcmV2ZXIiOmZhbHNlLCJpc3MiOiJodHRwOlwvXC9vbTIuZGZlLm9uZW1hcC5zZ1wvYXBpXC92MlwvdXNlclwvc2Vzc2lvbiIsImlhdCI6MTY0MDA5MTc1OSwiZXhwIjoxNjQwNTIzNzU5LCJuYmYiOjE2NDAwOTE3NTksImp0aSI6IjM2NWZhZDIzNDk1NDMyNjIzZTliOTdkMDU1YjM0NDIwIn0.9OFWv9fodyPTSpLhfbeOuPAgQiqy8gbDWt_VDAIAhZI`"


def get_planning_area_loc (lat, lon):
    get_pln_loc_url = "/getPlanningarea"
    params = {'token': ONEMAP_API_TOKEN, 'lat': lat, 'lng': lon}
    response = requests.get((base_url+get_pln_loc_url), params = params)
    if response.status_code != 200:
        raise ValueError
    pln_area_loc = response.json()
    return pln_area_loc[0]['pln_area_n']

def get_taxi_stand_stop_df():
    """
        Generate Taxi Stand/Stop/Pickup dataframe.
        Taxi Stand means taxis can wait for passengers,
        Taxi Stop means taxis only can pickup/alight passengers,
        Taxi Pick Up means taxis/cars can pickup/alight passengers.
    """

    taxi_stop_df = pd.DataFrame(gpd.read_file("raw_data/lta-taxi-stop-geojson.geojson"))

    taxi_stand_stop_list=[]
    for row in taxi_stop_df['Description']:
        taxi_type = str((re.findall(r"TAXI STAND|TAXI STOP|TAXI PICK UP", row))).strip("['']")
        taxi_stand_stop_list.append(taxi_type)
    taxi_stop_df['taxi_st_type'] = pd.Series(taxi_stand_stop_list)

    taxi_stop_df["taxi_st_lon"] = taxi_stop_df.geometry.map(lambda p: p.x)
    taxi_stop_df["taxi_st_lat"] = taxi_stop_df.geometry.map(lambda p: p.y)

    taxi_stop_df['taxi_pln_area'] = taxi_stop_df.apply(lambda x: get_planning_area_loc(x["taxi_st_lat"], x["taxi_st_lon"]), axis=1)

    return taxi_stop_df[['taxi_st_type','taxi_st_lat', 'taxi_st_lon', 'taxi_pln_area']]

if __name__ == "__main__":

    taxi_df = get_taxi_stand_stop_df()

    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.c_taxi_stand'

    job = client.load_table_from_dataframe(taxi_df, table_id)

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows,
                                                       len(table.schema),
                                                       table_id))
