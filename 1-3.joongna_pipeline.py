from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import requests
import json
import time
import re
import pandas as pd
import pymysql
import sys
import pendulum
from airflow.decorators import dag, task

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "start_date": datetime(2022, 7, 18, tzinfo=KST)
}


@dag(dag_id='1.joongna_mobile_pipeline',
    schedule_interval="0 10 * * *",
    default_args=default_args,
    tags=['crawler', 'joongna', 'pipeline'],
    catchup=False)

def taskflow():
    @task   
    def get_joongna_products(day):
        product_list = []
        category_list = ['1150', '1151']
        product_state_list = [-1, 3]
        url = 'https://search-api.joongna.com/v25/list/category'
        before_day = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d %H:%M:%S')
        for category_num in category_list:
            for product_state_num in product_state_list:
                page_num = -1
                while True:
                    if (page_num == 'end'):
                        break
                    else:
                        page_num += 1
                        payload = {
                            "filter": {
                                "categoryDepth": 3,
                                "categorySeq": category_num,
                                "color": "ZZZZZZ",
                                "dateFilterParameter": {
                                    "sortEndDate": '',
                                    "sortStartDate": ''
                                },
                                "flawedYn": 0,
                                "fullPackageYn": 0,
                                "limitedEditionYn": 0,
                                "maxPrice": 3000000,
                                "minPrice": 50000,
                                "platformType": 1,
                                "productCondition": -1,
                                "state": product_state_num,
                                "tradeType": -1
                                },
                                "osType": 2,
                                "searchQuantity": 12,
                                "searchStartTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                "sort": 0,
                                "startIndex": page_num,
                                "version": 1
                                }
                        time.sleep(1)
                        products = json.loads(requests.post(url, json=payload).text)
                        for div_num in range(12):
                            if(products['data']['items'][div_num]['sortDate'] < before_day):
                                page_num = 'end'
                                break
                            else :
                                product_info = products['data']['items'][div_num]
                                if(product_info['state'] == 3):
                                    product_state = '판매완료'
                                elif(product_info['state'] == 0):
                                    product_state = '판매중'
                                else:
                                    continue
                                if(products['data']['items'][div_num]['locationNames'] == []):
                                    product_location = '지역없음'
                                else:
                                    product_location = products['data']['items'][div_num]['locationNames'][0]
                                product_dict = {
                                    'product_code' : product_info['seq'],
                                    'product_title' : product_info['title'],
                                    'product_price' : product_info['price'],
                                    'product_date' : product_info['sortDate'],
                                    'product_store_code' : product_info['storeSeq'],
                                    'safety_pay' : product_info['jnPayBadgeFlag'],
                                    'product_state' : product_state,
                                    'product_location' : product_location                   
                                }
                                product_list.append(product_dict)
        return json.dumps(product_list, indent=4, sort_keys=True, default=str)

    @task
    def get_info(product_list: dict):
        product_list = json.loads(product_list)
        code_list = []
        info_list = []
        for code in product_list:
            code_list.append(code['product_code'])
        code_list = list(set(code_list))
        for product_code in code_list:
            url = f'https://web.joongna.com/product/detail/{product_code}'
            try:
                html = urlopen(url)
                time.sleep(0.7)
                bsObject = BeautifulSoup(html,"html.parser")
                product_info = bsObject.head.find("meta",{'name':'description'}).get('content').replace('\n'," ").replace('  ',' ')
            except:
                continue
            info_dict = {
                'product_code' : product_code,
                'product_info' : product_info
            }
            info_list.append(info_dict)
        no_info_df = pd.DataFrame(product_list)
        info_df = pd.DataFrame(info_list)
        add_info_df = pd.merge(info_df, no_info_df, how='left')
        add_info_products = add_info_df.to_dict('records')
        
        return json.dumps(add_info_products, indent=4, sort_keys=True, default=str)
    
    @task
    def insert_products(add_info_products):
        add_info_product_list = json.loads(add_info_products)
        conn = pymysql.connect(host = '54.64.211.139', 
                     database = 'joongna_db', 
                     user='jh', 
                     password='multi1234!')
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        for data in add_info_product_list:
            try:    
                insert_sql = '''
                INSERT INTO products
                VALUES(%(product_code)s,%(product_title)s,%(product_price)s,%(product_info)s,%(product_date)s,
                        %(product_store_code)s,%(safety_pay)s,%(product_state)s,%(product_location)s);
                '''
                cursor.execute(insert_sql, data)
                conn.commit()
            except:
                continue
        sell_code_list = [] 
        before_day = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')
        select_sql = f'''
            SELECT product_code FROM products
            WHERE product_date > "{before_day}"
            AND product_state = "판매중";
            '''
        cursor.execute(select_sql)
        product_code = cursor.fetchall()
        cursor.close()
        conn.close()

        return json.dumps(product_code, indent=4, sort_keys=True, default=str)

    @task
    def update_sold_products(product_code, day):
        product_code_list = json.loads(product_code)
        category_list = ['1150', '1151']
        sold_product_list = []
        url = 'https://search-api.joongna.com/v25/list/category'  
        before_day = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d %H:%M:%S')
        for category_num in category_list:
            page_num = -1
            while True:
                if (page_num == 'end'):
                    break
                else:
                    page_num += 1
                    payload = {
                        "filter": {
                            "categoryDepth": 3,
                            "categorySeq": category_num,
                            "color": "ZZZZZZ",
                            "dateFilterParameter": {
                                "sortEndDate": '',
                                "sortStartDate": ''
                            },
                            "flawedYn": 0,
                            "fullPackageYn": 0,
                            "limitedEditionYn": 0,
                            "maxPrice": 3000000,
                            "minPrice": 50000,
                            "platformType": 1,
                            "productCondition": -1,
                            "state": 3,
                            "tradeType": -1
                            },
                            "osType": 2,
                            "searchQuantity": 12,
                            "searchStartTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            "sort": 0,
                            "startIndex": page_num,
                            "version": 1
                            }
                    time.sleep(1)
            
                    products = json.loads(requests.post(url, json=payload).text)
                    for div_num in range(12):
                        if(products['data']['items'][div_num]['sortDate'] < before_day):
                            page_num = 'end'
                            break
                        else :
                            sold_product_list.append(products['data']['items'][div_num]['seq'])
        code_list = [x['product_code'] for x in product_code_list]
        change_state_list = [value for value in code_list if value in sold_product_list]
        
        conn = pymysql.connect(host = '54.64.211.139', 
                     database = 'joongna_db', 
                     user='jh', 
                     password='multi1234!')
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if (len(change_state_list) == 0):
            cursor.close()
            conn.close()
        else: 
            for update_code in change_state_list:
                update_sql = f'''
                    UPDATE products
                    SET product_state = '판매완료'
                    WHERE product_code = {update_code}
                '''
                cursor.execute(update_sql)
                conn.commit()
            cursor.close()
            conn.close()

        print(change_state_list)

    joongna_product = get_joongna_products(2)
    get_product_info = get_info(joongna_product)
    insert_data = insert_products(get_product_info)
    update_data = update_sold_products(insert_data, 30)

joongna_pipeline = taskflow()





