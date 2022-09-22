import json
import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pickle
from airflow.models import Variable
import catboost

def get_data():
    data = requests.get('http://ec2-65-2-37-66.ap-south-1.compute.amazonaws.com/data/current')
    # if not os.path.exists("Data"):
    #     os.mkdir("Data")
    #     with open('Data/data.json', 'w', encoding='utf-8') as f:
    #         json.dump(data.json(), f, ensure_ascii=False, indent=4)
    # else:
    #     with open('Data/data.json', 'w', encoding='utf-8') as f:
    #         json.dump(data.json(), f, ensure_ascii=False, indent=4)
    df=pd.DataFrame(data.json())
    df["date"]=pd.to_datetime(df["date"])
    df=df.set_index("date")
    df=df.resample('H').mean().interpolate(method='linear', limit_direction='forward')
    def create_windows(df, window_len, name):
        dic = {}
        for i in range(window_len-1, -1, -1):
            dic[f'{name} {i}'] = df.shift(i).values
        windows = pd.DataFrame(dic)
        windows.index = df.index
        return windows
    windows=create_windows(df.temperature,336,"Temp")
    windows=windows.dropna()
    shift = 5 * 24
    Y = pd.DataFrame()
    Y['Temp True'] = windows['Temp 0'].copy().shift(-shift)
    Y.index = Y.index.shift(shift)
    windows.index=windows.index.shift(shift)
    Y.dropna(inplace=True)
    windows.dropna(inplace=True)
    X=windows.loc[:"2022-09-04 23:00:00+00:00"]
    cat=catboost.CatBoostRegressor()
    cat.fit(X,Y)
    pickle.dump(cat,open("../plugins/catboost_model.pkl","wb"))
    x="model trained successfully"
    return x

default_args={
    "owner":"Mr Tudung"
}

with DAG(
    default_args=default_args,
    dag_id='imd_5_days',
    description='IMD Data tag',
    schedule_interval='0 0 * * 5',
    start_date=datetime(2021,9,5),
    catchup=False,
) as dag:
    task1=PythonOperator(
        task_id="get_store_data",
        python_callable=get_data
    )

    task1