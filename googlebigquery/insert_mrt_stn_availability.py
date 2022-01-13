import pandas as pd
import numpy as np
import requests
import datetime
from google.cloud import bigquery, storage

def get_first_time(update_time, stn_first_time):
    today_date = update_time.date()
    first_time = str(today_date) + ' ' + str(stn_first_time)
    return datetime.datetime.strptime(first_time, '%Y-%m-%d %H:%M:%S')

def get_last_time(update_time, stn_last_train):
    tmr_date = update_time.date()  + datetime.timedelta(days = 1)
    today_date = update_time.date()
    if datetime.datetime.strptime(stn_last_train, '%H:%M:%S').time() < datetime.time(2,0):
        last_time = str(tmr_date) + ' ' + str(stn_last_train)
    else:
        last_time = str(today_date) + ' ' + str(stn_last_train)
    return datetime.datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')

def check_operation_bool(update_time, start_train, last_train):
    if (start_train <= update_time) and (last_train >= update_time):
        return 1.0
    return 0.0

def get_mrt_status(request):
    """\
      Update mrt status based on operation time and any breakdowns
    """
    update_time = (datetime.datetime.now() + datetime.timedelta(hours=8))
    url = "http://datamall2.mytransport.sg/ltaodataservice/TrainServiceAlerts"
    DATA_MALL_API_ACC = "BehS/IpVR0KOFQ+BgFqM5g=="
    headers = {"AccountKey" : DATA_MALL_API_ACC}
    r = requests.get(url = url, headers=headers)

    BUCKET_NAME = 'static-file-storage'
    BUCKET_MRT_STN_LIST_PATH = 'mrtsg.csv'

    # Add Client() here
    storage_client = storage.Client()
    path = f"gs://{BUCKET_NAME}/{BUCKET_MRT_STN_LIST_PATH}"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(BUCKET_MRT_STN_LIST_PATH)

    with blob.open('r') as mrt_csv:
        # with open(public_path) as geofile:
        '''
        Take geojson file provided by LTA where the taxi stands coordinates are provided
        '''
        mrt_list_df = pd.read_csv(mrt_csv).fillna(1)
        print('loaded mrt_list file successfully')

    mrt_list_df = mrt_list_df.set_index('stn_id')
    status = r.json()["value"]["Status"]
    if status != 1:
        stations_list = []
        for d in r.json()["value"]["AffectedSegments"]:
            stations_list += d["Stations"].split(",")
        station_df = pd.DataFrame(stations_list, columns=["stn_id"])
        station_df["non_disruption_bool"] = 0
        station_df = station_df.set_index('stn_id')
        mrt_list_df.update(station_df)

    mrt_list_df.reset_index(inplace=True)

    mrt_list_df.loc[:,"stn_first_train_dt"] = mrt_list_df.apply(lambda x : get_last_time(update_time, x["stn_first_train"]), axis=1)
    mrt_list_df.loc[:,"stn_last_train_dt"] = mrt_list_df.apply(lambda x : get_last_time(update_time, x["stn_last_train"]), axis=1)
    mrt_list_df.loc[:,"in_operation_bool"] = mrt_list_df.apply(lambda x : check_operation_bool(update_time, x["stn_first_train_dt"], 
                                                                                        x["stn_last_train_dt"]), axis=1)
    mrt_list_df["final_status"] = mrt_list_df["in_operation_bool"] * mrt_list_df["non_disruption_bool"]
    mrt_list_df["update_time"] = update_time.strftime("%Y-%m-%d %H:%M:%S")
    mrt_list_df = mrt_list_df.drop(columns=["stn_first_train_dt","stn_last_train_dt"])

    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.h_mrt_status_availability'
    
    job = client.load_table_from_dataframe(
        mrt_list_df, table_id
    )

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    return ("Done!", 200)
