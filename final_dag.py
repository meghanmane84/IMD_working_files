import json
from tkinter import Y
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pickle
import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor
import matplotlib.pyplot as plt
from sktime.utils.plotting import plot_series
import os

from yaml import load

def get_data():
    data = requests.get('http://ec2-65-2-37-66.ap-south-1.compute.amazonaws.com/data/current?points=175392')
    if not os.path.exists("Data"):
        os.mkdir("Data")
        with open('Data/data.json', 'w', encoding='utf-8') as f:
            json.dump(data.json(), f, ensure_ascii=False, indent=4)
    else:
        with open('Data/data.json', 'w', encoding='utf-8') as f:
            json.dump(data.json(), f, ensure_ascii=False, indent=4)
    df=pd.DataFrame(data.json())
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date")
    de=df.loc['1989-01-01 01:00':'2018-01-01 00:00'].resample('H').meam().interpolate(method='linear', limit_direction='forward')
    df=df['1995-01-01 01:00':'2018-01-01 00:00']
    dataframe = df.copy()
    datetime_df = df.index.copy()
    for i in range(1, 336):
        dataframe.loc[:, f'temp_{i}'] = dataframe.temperature.shift(-i)
        datetime_df = datetime_df.shift(1)[:-1]
    dataframe.dropna(inplace=True)
    shift = 5 * 24
    dataframe['Temp_Pred_True'] = dataframe.temp_335.shift(-shift)
    datetime_df = datetime_df.shift(shift)[:-shift]
    dataframe.dropna(inplace=True)
    dataframe.index = datetime_df
    dataframe.loc[:'2016-12-31 23:00:00']['Temp_Pred_True']
    df.loc['1995-01-20 00:00:00':'2016-12-31 23:00:00']['temperature']
    train_df = dataframe.loc[:'2016-12-31 23:00:00']
    test_df = dataframe.loc['2017-01-01 00:00:00':]
    X_train=train_df.values[:, :-1]
    y_train=train_df.values[:, -1]
    gbm = LGBMRegressor(n_estimators=1000, random_state=1, n_jobs=100)
    gbm.fit(X_train,y_train)
    X=test_df.values[:, :-1]
    y=test_df.values[:,:-1]
    return X,y, gbm

def process_data(X,y,gbm):
    z=gbm.predict(X)
    return z

default_args={
    "owner":"Mr Tudung"
}

with DAG(
    default_args=default_args,
    dag_id='imd-dag-v3',
    description='IMD Data tag',
    schedule_interval='@daily',
    start_date=datetime(2022,7,22),
    catchup=False,

) as dag:
    task1=PythonOperator(
        task_id="get_store_data",
        python_callable=get_data
    )
    task2=PythonOperator(
        task_id="process_data",
        python_callable=process_data
    )
    task1 >> task2
