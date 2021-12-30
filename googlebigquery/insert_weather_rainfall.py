from google.cloud import bigquery
import pandas as pd
import numpy as np
import requests
import datetime

if __name__ == "__main__":
    gov = 'https://api.data.gov.sg/v1'
    weather_api = '/environment/rainfall'

    url = gov+weather_api
    response = requests.get(url).json()

    timestamp_str = response['items'][0]['timestamp']
    timestamp = str(datetime.datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S+08:00'))
    weather_list = []
    for index,value in enumerate(response['items'][0]['readings']):
        weather_list.append(
            {'station_id':response['metadata']['stations'][index]['id'], 
            'station_lat':response['metadata']['stations'][index]['location']['latitude'],
            'station_lon':response['metadata']['stations'][index]['location']['longitude'],
            'rainfall':float(response['items'][0]['readings'][index]['value']),
            'update_time': timestamp})

    weather_df = pd.DataFrame(weather_list)

    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.h_weather_rainfall'

    job = client.load_table_from_dataframe(
        weather_df, table_id
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