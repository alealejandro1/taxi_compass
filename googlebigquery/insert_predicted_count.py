import pandas as pd
import numpy as np

import googleapiclient.discovery
from google.api_core.client_options import ClientOptions

from google.cloud import bigquery

def get_data():
    bqclient = bigquery.Client()

    # Download query results.
    query_string = """
    select x.taxi_st_id,  substr(x.taxi_st_id,5) taxi_st_num,  x.taxi_count, x.taxi_update_time, x.weather_stn_id, c.rainfall, c.weather_update_time, x.mrt_stn_id, e.mrt_final_status, e.mrt_update_time
    from (
    select a.taxi_st_id, a.taxi_count, a.taxi_update_time, b.weather_stn_id, d.mrt_stn_id
    from (
    SELECT ts_id as taxi_st_id, taxi_count, cast(timestamp_trunc(timestamp, minute) as datetime) as taxi_update_time
    FROM `taxi-compass-lewagon.api_dataset.h_taxi_stand_taxi_count`
    WHERE timestamp > TIMESTAMP_add(CURRENT_TIMESTAMP() , INTERVAL 464 minute)
    ) a
    left join
    (
    select weather_stn_id, taxi_st_id from `taxi-compass-lewagon.api_dataset.c_taxi_stand_weather_stn`
    ) b on a.taxi_st_id = b.taxi_st_id
    left join 
    (
    select taxi_st_id, mrt_stn as mrt_stn_id from `taxi-compass-lewagon.api_dataset.c_mrt_stn_taxi_stand`
    where mrt_stn is not null
    ) d on a.taxi_st_id = d.taxi_st_id
    )x
    left join 
    (
    select station_id as weather_stn_id, rainfall, datetime_trunc(datetime (update_time), minute) as weather_update_time
    from `taxi-compass-lewagon.api_dataset.h_weather_rainfall`
    where datetime(update_time) > datetime_SUB(CURRENT_DATETIME() , INTERVAL 1 hour)
    ) c on x.weather_stn_id = c.weather_stn_id and x.taxi_update_time = c.weather_update_time
    left join
    (
    select stn_id as mrt_stn_id, final_status as mrt_final_status, datetime_trunc(datetime (update_time), minute) as mrt_update_time 
    from `taxi-compass-lewagon.api_dataset.h_mrt_status_availability`
    where datetime(update_time) > datetime_SUB(CURRENT_DATETIME() , INTERVAL 1 hour)
    ) e on x.taxi_update_time = e.mrt_update_time and x.mrt_stn_id = e.mrt_stn_id
    """

    taxi_df_pred = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    
    print("gbq query successful...")
    return taxi_df_pred

def get_weekday(time):
    time = time.weekday()
    if time == 5 or time == 6:
        return 1
    return 0

def preprocessing(taxi_df_pred):
    df = taxi_df_pred[["taxi_st_num","taxi_update_time","taxi_count","rainfall","mrt_final_status"]].copy()
    df["taxi_st_num"] = df["taxi_st_num"].astype('int64')
    df = df.sort_values(by=["taxi_st_num", "taxi_update_time"],ascending=True).reset_index(drop=True)
    df[["rainfall"]] = df[["rainfall"]].fillna(df.groupby(['taxi_st_num'])[["rainfall"]].ffill())
    df[["mrt_final_status","rainfall"]] = df[["mrt_final_status","rainfall"]].fillna(value=0)
    df = df.groupby(["taxi_st_num","taxi_update_time","taxi_count","rainfall"]).agg('min').reset_index().drop_duplicates(subset=["taxi_st_num","taxi_update_time"])
    df = df.set_index(["taxi_st_num","taxi_update_time"])
    df = df.groupby(level=0).apply(lambda x: x.reset_index(level=0, drop=True).asfreq("60S")).reset_index()
    df[["taxi_count","rainfall","mrt_final_status"]] = df[["taxi_count","rainfall","mrt_final_status"]].fillna(df.groupby(['taxi_st_num'])[["taxi_count","rainfall","mrt_final_status"]].ffill())
    df["hour"] = df["taxi_update_time"].dt.hour
    df["minute"] = df["taxi_update_time"].dt.minute
    df['hr_sin'] = np.sin(df["hour"]*(2.*np.pi/24))
    df['hr_cos'] = np.cos(df["hour"]*(2.*np.pi/24))
    df['min_sin'] = np.sin(df["minute"]*(2.*np.pi/60))
    df['min_cos'] = np.cos(df["minute"]*(2.*np.pi/60))
    df["taxi_update_time"] = df["taxi_update_time"].dt.tz_localize("Asia/Singapore")
    df["weekend_bool"] = df.apply(lambda x : get_weekday(x["taxi_update_time"]), axis=1)
    
    print("preprocessing succesful...")
    
    return df

def array_creation(df):
    X_mas = np.array([])
    X_mas_pred = pd.DataFrame()

    bins = [0, 1, 2, 3, 4, 10000]
    labels = [0, 1, 2, 3, 4]
    for i in range(350):
        print(i+1,len(df.loc[df["taxi_st_num"] == i+1]), "started...")
        X = df.loc[df["taxi_st_num"] == i+1][["taxi_st_num","taxi_update_time","taxi_count", "rainfall","mrt_final_status",
                                            "weekend_bool","hr_sin","hr_cos","min_sin","min_cos"]].copy()

        X = X.dropna()
        X_pred = X[["taxi_st_num","taxi_update_time"]]
        
        X = X.drop(columns=["taxi_update_time"]).to_numpy()
        
        X = X.reshape(1, X.shape[0], X.shape[1])
        
        if len(X_mas) == 0:
            X_mas = X
            X_mas_pred = X_pred
        else:
            X_mas = np.vstack((X_mas, X))
            X_mas_pred = pd.concat([X_mas_pred, X_pred],ignore_index=True)

    print("array creation successful...")
    
    return (X_mas, X_mas_pred)

def predict_json(project, region, model, instances, version=None):
    """Send json data to a deployed model for prediction.

    Args:
        project (str): project where the Cloud ML Engine Model is deployed.
        region (str): regional endpoint to use; set to None for ml.googleapis.com
        model (str): model name.
        instances ([Mapping[str: Any]]): Keys should be the names of Tensors
            your deployed model expects as inputs. Values should be datatypes
            convertible to Tensors, or (potentially nested) lists of datatypes
            convertible to tensors.
        version: str, version of the model to target.
    Returns:
        Mapping[str: any]: dictionary of prediction results defined by the
            model.
    """
    # Create the ML Engine service object.
    # To authenticate set the environment variable
    # GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_file>
    prefix = "{}-ml".format(region) if region else "ml"
    api_endpoint = "https://{}.googleapis.com".format(prefix)
    client_options = ClientOptions(api_endpoint=api_endpoint)
    service = googleapiclient.discovery.build(
        'ml', 'v1', client_options=client_options)
    name = 'projects/{}/models/{}'.format(project, model)

    if version is not None:
        name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body={'instances': instances}
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return response['predictions']

def predict(X_mas, X_mas_pred):
    PROJECT_ID = 'taxi-compass-lewagon'
    REGION = "asia-southeast1"
    MODEL = "xgb_class_model"
    MODEL_VERSION = "version3"
    
    X_mas1 = X_mas.reshape(X_mas.shape[0]*X_mas.shape[1], 9)
    pred = predict_json(PROJECT_ID, REGION, MODEL, X_mas1.tolist() , version=MODEL_VERSION)
    X_mas_pred["timestamp_pred"] = X_mas_pred["taxi_update_time"].dt.tz_localize(None) + pd.to_timedelta(15, unit='m')
    X_mas_pred["timestamp_pred"] = X_mas_pred["timestamp_pred"].astype(str)
    X_mas_pred["taxi_st_id"] = "kml_" + X_mas_pred["taxi_st_num"].astype("str")
    y_res = pd.concat((X_mas_pred[["taxi_st_id","timestamp_pred"]], pd.DataFrame(pred, columns=["taxi_count_pred"])), axis=1)
    y_res["minute"] = y_res.groupby(['taxi_st_id']).cumcount()+1
    
    print("predict successful...")
    
    return y_res

def delete_all_rows():
    client = bigquery.Client()

    dml_statement = (
        "delete from `taxi-compass-lewagon.api_dataset.r_taxi_stand_pred` WHERE true")
    query_job = client.query(dml_statement)  # API request
    query_job.result()  # Waits for statement to finish
    
    print("delete successful...")
    
    return "delete successful..."
    
def insert_rows(y_res):
    client = bigquery.Client(project='taxi-compass-lewagon')
    table_id = 'api_dataset.r_taxi_stand_pred'
    
    job = client.load_table_from_dataframe(
        y_res, table_id
    )

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    print("insert successful...")
    
    return "insert successful..."

def predicted_count():
    taxi_df_pred = get_data()
    df = preprocessing(taxi_df_pred)
    X_mas, X_mas_pred = array_creation(df)
    y_res = predict(X_mas, X_mas_pred)
    delete_all_rows()
    insert_rows(y_res)
    
    print("all successful...")
    
    return ("Done!", 200)

if __name__ == "__main__":
    predicted_count()
    