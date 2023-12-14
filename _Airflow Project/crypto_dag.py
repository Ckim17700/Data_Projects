#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import s3fs
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from coinmarketcap_script import run_api


# In[2]:


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 14),
    'email': ['ckim17700@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }


# In[3]:


dag = DAG(
    'crypto_dag',
    default_args=default_args,
    description='Crypto ETL DAG',
    schedule_interval=timedelta(days=1),
    )


# In[4]:


run_etl = PythonOperator(
    task_id='crypto_etl',
    python_callable=run_api,
    dag=dag, 
    )


# In[5]:


run_api()


# In[ ]:




