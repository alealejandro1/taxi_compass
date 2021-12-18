from google.cloud import bigquery
import pandas as pd
import numpy as np
import requests
import datetime

"""
    To query taxi location, every 5 minutes, then input to BigQuery
"""

if __name__ == "__main__":

    # Prepare dataframe
    taxi_url = "https://api.data.gov.sg/v1/transport/taxi-availability"
    r = requests.get(taxi_url)
    coordinates = r.json()["features"][0]["geometry"]["coordinates"]
    timestamp = r.json()["features"][0]["properties"]["timestamp"]
    taxi_available = pd.DataFrame(np.array(coordinates), columns=["lon","lat"])
    taxi_available["update_time"] = str(datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%S+08:00"))

    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.h_taxi_availability'
    
    job = client.load_table_from_dataframe(
        taxi_available, table_id
    )

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    
# requirements: 
# google-cloud-bigquery==2.31.0
# numpy==1.18.5
# pandas==1.3.1
# requests==2.26.0
# pyarrow==6.0.1