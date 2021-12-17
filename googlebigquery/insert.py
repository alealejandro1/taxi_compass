from google.cloud import bigquery
import pandas as pd
import numpy as np
import requests
import datetime

"""
    To query taxi location, every 5 minutes, then input to BigQuery
"""

# Prepare dataframe
taxi_url = "https://api.data.gov.sg/v1/transport/taxi-availability"
r = requests.get(taxi_url)
coordinates = r.json()["features"][0]["geometry"]["coordinates"]
timestamp = r.json()["features"][0]["properties"]["timestamp"]
taxi_available = pd.DataFrame(np.array(coordinates), columns=[["lon","lat"]])
taxi_available["update_time"] = str(datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%S+08:00"))

client = bigquery.Client()
table_id = 'my_dataset.new_table'

# to ensure the correct BigQuery data type.
job_config = bigquery.LoadJobConfig(schema = [
    bigquery.SchemaField("lat", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("lon", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("update_time", "STRING", mode="REQUIRED"),
])

job = client.load_table_from_dataframe(
    taxi_available, table_id, job_config=job_config
)

job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)