from airflow.decorators import dag, task
import pendulum
from datetime import datetime, timedelta
import pymysql
import json
import pandas as pd
import re

KST = pendulum.timezone("Asia/Seoul")

def get_connect_db(db_name):
    conn = pymysql.connect(host = '54.64.211.139',
                     database = f'{db_name}',
                     user='jh',
                     password='multi1234!')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor


default_args = {
    "start_date": datetime(2022, 7, 18, tzinfo=KST)
}

@dag(dag_id="2.pro_bunjang_db",
    schedule_interval= "0 14 * * *",
    default_args=default_args,
    tags=["Bunjang", "DB", "pipeline"],
    catchup=False)

def taskflow():

    @task()
    def get_data():
        conn, cursor = get_connect_db('bunjang_db')
        select_sql = '''
            SELECT
                store_code AS "product_code",
                title AS "product_title",
                price AS "product_price",
                content AS "product_info",
                update_time AS "product_date",
                location AS "product_location"
            FROM 
                bunjang_table
            '''
        cursor.execute(select_sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
    
        return json.dumps(result, indent=4, sort_keys=True, default=str)

    @task()
    def insert_data(result):
        result = json.loads(result)
        df = pd.DataFrame(result)
        df.insert(1, 'product_domain', '번개장터')
        df.insert(1, 'product_state', True)
        df.drop_duplicates(['product_code'], inplace=True)
        datas_dict = df.to_dict('records')

        conn, cursor = get_connect_db('product_db')
        for data in datas_dict:
            try:
                insert_sql = '''
                        INSERT INTO bunjang_table
                        VALUES(%(product_code)s,%(product_domain)s,%(product_title)s,%(product_price)s,
                        %(product_info)s,%(product_date)s,%(product_state)s,%(product_location)s);
                        '''
                cursor.execute(insert_sql, data)
                conn.commit()
            except:
                continue
        cursor.close()
        conn.close()

    get_joongna_data = get_data()
    insert_data_ = insert_data(get_joongna_data)

to_master_db_flow = taskflow()