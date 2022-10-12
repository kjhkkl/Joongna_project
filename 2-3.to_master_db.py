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

@dag(dag_id='2.move_data_dag',
    default_args=default_args,
    schedule_interval="0 12 * * *",
    tags=['preprocessing', 'joongna'],
    catchup=False)

def taskflow():

    @task()
    def get_data(day):
        before_day = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d %H:%M:%S')
        conn, cursor = get_connect_db('joongna_db')
        select_sql = f'''
            SELECT * FROM products
            WHERE product_date > "{before_day}";
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
        df.insert(1, 'product_domain', '중고나라')
        df.drop(columns=["product_store_code", "safety_pay"],inplace=True)
        df.loc[df['product_state'] == '판매중', 'product_state'] = False
        df.loc[df['product_state'] == '판매완료', 'product_state'] = True
        datas_dict = df.to_dict('records')

        conn, cursor = get_connect_db('product_db')
        for data in datas_dict:
            try:
                insert_sql = '''
                        INSERT INTO joongna_table
                        VALUES(%(product_code)s,%(product_domain)s,%(product_title)s,%(product_price)s,
                        %(product_info)s,%(product_date)s,%(product_state)s,%(product_location)s);
                        '''
                cursor.execute(insert_sql, data)
                conn.commit()
            except:
                continue
        cursor.close()
        conn.close()

        conn, cursor = get_connect_db('joongna_db')
        before_day = (datetime.now() - timedelta(days=21)).strftime('%Y-%m-%d %H:%M:%S')
        select_sql = f'''
            SELECT product_code FROM products
            WHERE product_date > "{before_day}"
            AND product_state = "판매완료";
            '''
        cursor.execute(select_sql)
        sold_code = cursor.fetchall()
        sold_code_list = [x['product_code'] for x in sold_code]
        cursor.close()
        conn.close()

        conn, cursor = get_connect_db('product_db')
        select_sql = f'''
            SELECT product_code FROM joongna_table
            WHERE product_date > "{before_day}"
            AND product_state = False;
            '''
        cursor.execute(select_sql)
        onsale_code = cursor.fetchall()
        onsale_code_list = [x['product_code'] for x in onsale_code]
        update_state_list = [value for value in onsale_code_list if value in sold_code_list]
        if (len(update_state_list) == 0):
            cursor.close()
            conn.close()
        else: 
            for update_code in update_state_list:
                update_sql = f'''
                    UPDATE joongna_table
                    SET product_state = True
                    WHERE product_code = {update_code};
                '''
                cursor.execute(update_sql)
                conn.commit()
            cursor.close()
            conn.close()

    get_joongna_data = get_data(4)
    insert_data_ = insert_data(get_joongna_data)

to_master_db_flow = taskflow()