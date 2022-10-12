import pandas as pd
import numpy as np
import pymysql
import re
from collections import Counter
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pymysql
from pymysql import Error
from airflow import DAG
from datetime import datetime, timedelta
from pytz import timezone
from airflow.operators.python import PythonOperator
import time
import pendulum
import json
from pandas import json_normalize

def get_data():

    HOST = "54.64.211.139"
    PORT = 3306
    phone_db = pymysql.connect(
        user= 'hm',
        passwd='multi1234!',
        host=HOST,
        port=PORT,
        db='daangn_db'
    )
    cursor = phone_db.cursor(pymysql.cursors.DictCursor)
    sql = \
    '''SELECT
    store_code AS "product_code",
    title AS "product_title",
    price AS "product_price",
    content AS "product_info",
    crawl_time AS "product_date",
    product_state,
    location AS "product_location"
    FROM daangn_table'''

    cursor.execute(sql)
    result = cursor.fetchall()
    df = pd.DataFrame(result)
    df=df.assign(product_domain='당근마켓')
    df = df[['product_code','product_domain','product_title','product_price','product_info','product_date','product_state','product_location']]
    df.drop_duplicates(['product_code'])
    today = datetime.today().strftime("%Y-%m-%d")
    print(today)
    mask = (df['product_date'] >= today)
    filtered = df.loc[mask]
    filtered = filtered.to_json()

    return filtered

def send_data(ti):
    dbdb = ti.xcom_pull(task_ids=['get_data'])[0]
    dbdb = json.loads(dbdb)
    ip_df = pd.DataFrame.from_dict(dbdb, orient = 'index').T
    #print(ip_df)
    db_connection_str = 'mysql+pymysql://hm:multi1234!@54.64.211.139:3306/product_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    conn = db_connection.connect()
    try:
        ip_df.to_sql(name='daangn_table', con=db_connection, if_exists='append', index=False)
    except Exception as e:
        print('오류거나 이미 한 행동', e)
    conn.close()

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": datetime(2022, 7, 18, tzinfo=KST)
}

with DAG(
    dag_id="2.pro_daggn_db",
    schedule_interval= "0 14 * * *",
    default_args=default_args,
    tags=["daangn", "DB", "pipeline"],
    catchup=False) as dag:

    
    get_Data = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )

    send_Data = PythonOperator(
        task_id = "send_data",
        python_callable=send_data
    )


    get_Data >> send_Data