from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
from datetime import datetime
from pymysql import Error
import pymysql
import pendulum

def get_href(search):
    url = 'https://api.bunjang.co.kr/api/1/find_v2.json?'
    headers = {'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.49'}
    href = []
    for page in range(300):
        try:
            payload = {'q': f'{search}',
                       'order': 'score',
                       'page': f'{page}',
                       'stat_device': 'w',
                       'n': '100',
                       'stat_category_required': '1',
                       'req_ref': 'search',
                       'version': '4'
                      }

            res = requests.get(url, params=payload, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            site_json = json.loads(soup.text)
            lst = site_json['list']
            for item in lst:
                try:
                    if item['status'] == '1':
                        href.append(item['pid'])
                except:
                    pass
        except:
            pass
        time.sleep(1)
    
    href = list(set(href)) # 중복 제거

    connection = pymysql.connect(host = '54.64.211.139', database = 'bunjang_db', user='jp', password='multi1234!')
    cursor = connection.cursor()
    query = """SELECT * FROM code_list"""
    cursor.execute(query)
    res = cursor.fetchall()

    try:
        ls = [int(item[0]) for item in res]
    except:
        ls = []

    try:
        href = list(set(href) - set(ls))
    except:
        pass

    try:
        connection = pymysql.connect(host = '54.64.211.139', database = 'bunjang_db', user='jp', password='multi1234!')
        cursor = connection.cursor()
        for code in href:
            query = f"""INSERT INTO code_list (store_code) Values (%s)"""
            cursor.execute(query, code)
            connection.commit()
    except Error as e:
        print('Error : ', e)

    return json.dumps(href)

def bunjang_crawler(href_list):
    df = None
    for h in href_list:
        time.sleep(0.8)
        try:
            url = f'https://api.bunjang.co.kr/api/1/product/{int(h)}/detail_info.json?version=4'
            headers = {'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36 Edg/103.0.1264.49'}
            res = requests.get(url, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            site_json = json.loads(soup.text)
            try:
                store_code = int(site_json['item_info']['pid'])
            except:
                store_code = None
            try:
                title = site_json['item_info']['name']
            except:
                title = None
            try:
                price = site_json['item_info']['price']
            except:
                price = None
            try:
                content = site_json['item_info']['description']
            except:
                content = None
            try:
                image_count = site_json['item_info']['image_count']
            except:
                image_count = None
            try:
                keyword = site_json['item_info']['keyword']
            except:
                keyword = None
            try:
                location = site_json['item_info']['location']
            except:
                location = None
            try:   
                update_time = datetime.fromtimestamp((site_json['item_info']['update_time'])).strftime("%Y-%m-%d %H:%M:%S")
            except:
                update_time = None            
            a = pd.DataFrame([[store_code, location, title, price, content, image_count, keyword, update_time]], columns = ['store_code', 'location', 'title','price','content','image_count','keyword','update_time'])
            df = pd.concat([df, a])
        except:
            pass
    try:
        df = df.reset_index(drop=True)
    except:
        pass
    return df

def send_iphone_data(ti):
    iphone_href = ti.xcom_pull(task_ids=['get_href_iphone'])[0]
    iphone_href = json.loads(iphone_href)
    db_connection_str = 'mysql+pymysql://jp:multi1234!@54.64.211.139:3306/bunjang_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    ip_df = bunjang_crawler(iphone_href)
    conn = db_connection.connect()
    try:
        ip_df.to_sql(name='bunjang_table', con=db_connection, if_exists='append', index=False)
    except:
        print('sql삽입 에러발생')
    conn.close()

def send_galaxy_data(ti):
    galaxy_href = ti.xcom_pull(task_ids=['get_href_galaxy'])[0]
    galaxy_href = json.loads(galaxy_href)
    db_connection_str = 'mysql+pymysql://jp:multi1234!@54.64.211.139:3306/bunjang_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    ip_df = bunjang_crawler(galaxy_href)
    conn = db_connection.connect()
    try:
        ip_df.to_sql(name='bunjang_table', con=db_connection, if_exists='append', index=False)
    except:
        print('sql삽입 에러발생')
    conn.close()

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": datetime(2022, 7, 13, tzinfo=KST)
}

with DAG(
    dag_id="1.bunjang_pipeline",
    schedule_interval= "0 10 * * *",
    default_args=default_args,
    tags=["bunjang", "crawler", "pipeline"],
    catchup=False) as dag:
    
    get_href_iphone = PythonOperator(
        task_id="get_href_iphone",
        python_callable=get_href,
        op_kwargs={"search" : "아이폰"}
    )

    send_iphone_DATA = PythonOperator(
        task_id = "send_iphone_DATA",
        python_callable=send_iphone_data
    )

    get_href_galaxy = PythonOperator(
        task_id="get_href_galaxy",
        python_callable=get_href,
        op_kwargs={"search" : "갤럭시"}
    )

    send_galaxy_DATA = PythonOperator(
        task_id = "send_galaxy_DATA",
        python_callable=send_galaxy_data
    )

    [get_href_iphone >> send_iphone_DATA ,get_href_galaxy >> send_galaxy_DATA] 