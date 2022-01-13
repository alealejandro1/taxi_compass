import pandas as pd
import numpy as np
import requests
from math import radians, sin, cos, asin, sqrt
import datetime

from google.cloud import bigquery, storage

def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Compute distance between two pairs of coordinates (lon1, lat1, lon2, lat2)
    See - (https://en.wikipedia.org/wiki/Haversine_formula)
    Distance is measured in kilometers when r = 6371
    r = 6371  Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    Lats and Longs are converted to radians first then computed used haversine
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    radius = 6371
    return 2 * radius * asin(sqrt(a))

if __name__ == "__main__":
    bqclient = bigquery.Client()

    # Download query results.
    query_string = """
    select distinct taxi_st_id, taxi_st_lat, taxi_st_lon from `taxi-compass-lewagon.api_dataset.c_taxi_stand`
    order by taxi_st_id asc
    """

    taxi_st_df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    
    bqclient = bigquery.Client()

    # Download query results.
    query_string = """
    select distinct a.station_id, a.station_lat, a.station_lon from `taxi-compass-lewagon.api_dataset.h_weather_rainfall` a
    order by station_id asc
    """

    weather_df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    
    combined_df = pd.merge(taxi_st_df, weather_df, how="cross")
    combined_df["distance"] = combined_df.apply(lambda x : haversine_distance(x["station_lon"], x["station_lat"], 
                                                                          x["taxi_st_lon"], x["taxi_st_lat"]), axis=1)
    combined_df = combined_df.sort_values('distance', ascending=True).drop_duplicates(["taxi_st_id"])[["taxi_st_id","station_id"]]
    combined_df.reset_index(inplace=True, drop=True)
    combined_df = combined_df.rename(columns={"station_id" : "weather_stn_id"})
    
    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.c_taxi_stand_weather_stn'

    job = client.load_table_from_dataframe(
        combined_df, table_id
    )

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )