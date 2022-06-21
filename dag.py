#importing packages
from select import select
from tkinter import EXCEPTION
import sqlite3
from airflow import DAG, utils, configuration
from airflow.operators import bash_operator, branch_operator, dummy_operator, email_operator, python_operator
from datetime import date, datetime, time, timedelta
from airflow.utils.state import State
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import os

#for data importing


#actual DAG codes

#to check its daily working
def trigger_alert():
    raise EXCEPTION('Daily alert system test')

#to read the data
def read() :
    con = sqlite3.connect('Station_43057.db')
    with con:
        df = pd.read_sql('SELECT * FROM Data', con, parse_dates="DateTime", coerce_float=True, index_col="DateTime")
    df


#to create ML algo
from ML import eval_and_save as evsa 
def ml() :
    evsa

#Data Processing
from data_process import dataframe as dpc
def data_processing() :
    dpc

#to alert when the task is completed
def unfail_task(context):
    ti=context['ti']
    ti.state = State.SUCCESS    

#basic dag file
dag= DAG(
    dag_id='imd_prototype',
    description='Test alert',
    schedule_interval='@daily',
    start_date=datetime(2021, 9, 5),
    default_args={ 
        'owner': 'airflow',
        'retries':'0',
    },
    catchup=False,
)    

#basic task operation file
op= python_operator(
    task_id='op',
    python_callable=trigger_alert,
    dag=dag,
    execution_timeout=timedelta(seconds=60),
    priority_weight=100,
    on_failure_callback=unfail_task,
)

rd= python_operator(
    task_id='rd',
    python_callable = read,
    dag=dag,
    execution_timeout = timedelta(seconds =120),
    priority_weight=500,
    on_failure_callback=unfail_task,
)

dproc= python_operator(
    task_id='dproc',
    python_callable = data_processing,
    dag=dag,
    execution_timeout = timedelta(seconds =120),
    priority_weight=500,
    on_failure_callback=unfail_task,
)

mL= python_operator(
    task_id='mL',
    python_callable = ml,
    dag=dag,
    execution_timeout = timedelta(seconds =120),
    priority_weight=500,
    on_failure_callback=unfail_task,
)

op >> rd >> dproc >> mL
