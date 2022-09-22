import json
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pickle
import catboost
from airflow.models import Variable

def predict_hourly_datapoint():
    data = requests.get('http://ec2-65-2-37-66.ap-south-1.compute.amazonaws.com/data/current?points=336')
    df=pd.DataFrame(data.json())
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date")
    df = df.resample('H').mean().interpolate(method='linear', limit_direction='forward')
    dataframe = df.copy()
    datetime_df = df.index.copy()
    for i in range(1, 336):
        dataframe.loc[:, f'temp_{i}'] = dataframe.temperature.shift(i)
        datetime_df = datetime_df.shift(1)[:-1]
    dataframe.dropna(inplace=True)
    model=pickle.load(open("../plugins/catboost_model.pkl","rb"))
    data_point=model.predict(dataframe)
    day=datetime_df[len(datetime_df)-1]+datetime.timedelta(days=5)
    data_insert=[[day,data_point]]
    pred_data=pd.DataFrame(data_insert,columns=["date","temp"])
    pred_data.set_index("date")
    return pred_data

default_args={
    "owner":"Mr Tudung"
}

with DAG(
    default_args=default_args,
    dag_id='imd_hourly_dag',
    description='IMD Data tag',
    schedule_interval='@hourly',
    start_date=datetime(2021,9,5),
    catchup=False,
) as dag:
    task1=PythonOperator(
        task_id="get_store_data",
        python_callable=predict_hourly_datapoint
    )

    task1