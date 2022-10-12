from datetime import datetime
from airflow import DAG
from bs4 import BeautifulSoup
import requests
from pandas import json_normalize
import numpy as np
import pandas as pd
import json
from datetime import datetime, timedelta
from pytz import timezone
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import create_engine
import pymysql
from pymysql import Error
import pendulum

def get_href(search_word, page_start, page_end):
    href = []
    for i in range(int(page_start),int(page_end) + 1):
        try:
            res = requests.get(f'https://www.daangn.com/search/{search_word}/more/flea_market?page={i}')
            soup = BeautifulSoup(res.text,'html.parser')
            link_lst = soup.select('a.flea-market-article-link')
            for j in link_lst:
                try:
                    code = j.attrs['href']
                    code = code.split('/')[-1]
                    href.append(int(code))
                except:
                    pass
            time.sleep(1)
        except:
            print('에러 발생')

    href = list(set(href)) # 중복제거

    connection = pymysql.connect(host = '54.64.211.139', database = 'daangn_db', user='jp', password='multi1234!')
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
        connection = pymysql.connect(host = '54.64.211.139', database = 'daangn_db', user='jp', password='multi1234!')
        cursor = connection.cursor()
        for code in href:
            query = f"""INSERT INTO code_list (store_code) Values (%s)"""
            cursor.execute(query, code)
            connection.commit()
    except Error as e:
        print('Error : ', e)
        
    return json.dumps(href)

def daangn_crawler(href_list):
    df = None
    for h in href_list:
        time.sleep(0.8)
        try:
            url = f'https://www.daangn.com/articles/{str(h)}'
            res = requests.get(url)
            soup = BeautifulSoup(res.text,'html.parser')

            code = h
            title = soup.select_one('#article-title').text
            price = int(soup.select_one('#article-price').text.strip().replace(',','').replace('원','').replace('만',''))
            category = soup.select_one('#article-category').text.strip().replace('\n          \n           ','').split(' ∙ ')[0]
            elapsed_time = soup.select_one('#article-category').text.strip().replace('\n          \n           ','').split(' ∙ ')[1]
            crawl_time = time.strftime('%Y-%m-%d-%H:%M', time.localtime(time.time()))
            content = soup.select_one('#article-detail > p').text.replace('\n','')
            like = int(soup.select_one('#article-counts').text.split('∙')[0].strip().split(' ')[1])
            chat = int(soup.select_one('#article-counts').text.split('∙')[1].strip().split(' ')[1])
            view = int(soup.select_one('#article-counts').text.split('∙')[2].strip().split(' ')[1])
            location = soup.select_one('#region-name').text
            in_stock = soup.find('meta', attrs={'name':'product:availability'})['content']
            if in_stock == 'oos':
                product_state = True
            else:
                product_state = False
            
            lst = [code, title, price, category, crawl_time, elapsed_time, content, like, chat, view, location, product_state]
            if product_state == True:
                df = pd.concat([df,pd.DataFrame([lst],
                columns = ['store_code' ,'title', 'price', 'category', 'crawl_time', 'elapsed_time', 'content', 'like_count', 'chat_count', 'view_count', 'location','product_state'])])
            else:
                pass
        except:
            pass
    df = df.reset_index(drop=True)
    return df

def send_iphone_data(ti):
    iphone_href = ti.xcom_pull(task_ids=['get_href_iphone'])[0]
    iphone_href = json.loads(iphone_href)
    db_connection_str = 'mysql+pymysql://jp:multi1234!@54.64.211.139:3306/daangn_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    ip_df = daangn_crawler(iphone_href)
    conn = db_connection.connect()
    ip_df.to_sql(name='daangn_table', con=db_connection, if_exists='append', index=False)
    conn.close()

def send_galaxy_data(ti):
    galaxy_href = ti.xcom_pull(task_ids=['get_href_galaxy'])[0]
    galaxy_href = json.loads(galaxy_href)
    db_connection_str = 'mysql+pymysql://jp:multi1234!@54.64.211.139:3306/daangn_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    ip_df = daangn_crawler(galaxy_href)
    conn = db_connection.connect()
    ip_df.to_sql(name='daangn_table', con=db_connection, if_exists='append', index=False)
    conn.close()

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": datetime(2022, 7, 13, tzinfo=KST)
}

with DAG(
    dag_id="1.daangn_pipeline",
    schedule_interval= "0 10 * * *",
    default_args=default_args,
    tags=["daangn", "crawler", "pipeline"],
    catchup=False) as dag:

    # task_start = BashOperator(
    # task_id='start',
    # bash_command='date'
    # )
    
    get_href_iphone = PythonOperator(
        task_id="get_href_iphone",
        python_callable=get_href,
        op_kwargs={"search_word" : "아이폰", "page_start" : 1, "page_end": 833}
    )

    send_iphone_DATA = PythonOperator(
        task_id = "send_iphone_DATA",
        python_callable=send_iphone_data
    )

    get_href_galaxy = PythonOperator(
        task_id="get_href_galaxy",
        python_callable=get_href,
        op_kwargs={"search_word" : "갤럭시", "page_start" : 1, "page_end": 833}
    )

    send_galaxy_DATA = PythonOperator(
        task_id = "send_galaxy_DATA",
        python_callable=send_galaxy_data
    )

    [get_href_iphone >> send_iphone_DATA ,get_href_galaxy >> send_galaxy_DATA] 