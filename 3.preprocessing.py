from airflow.decorators import dag, task
import pendulum
from datetime import datetime, timedelta
import pymysql
import json
import pandas as pd
import re

KST = pendulum.timezone("Asia/Seoul")
TABLE_LIST = ['daangn_table','bunjang_table','joongna_table']
DELETE_WORD = '교환|삽니다|구매|구입|매입|버즈|에어팟|탭|북|book|tab|패드|워치|pad|필름|케이스|케이블|lg|usb|충전기|g8|g900|wing|q|x2|벨벳|v50|v30|x6|x5|x4|g7'

default_args = {
    "start_date": datetime(2022, 7, 18, tzinfo=KST)
}

def get_connect_db(db_name):
    conn = pymysql.connect(host = '54.64.211.139',
                     database = f'{db_name}',
                     user='jh',
                     password='multi1234!')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor

@dag(dag_id='3.preprocessing_dag',
    default_args=default_args,
    schedule_interval='0 15 * * *',
    tags=['preprocessing'],
    catchup=False)

def taskflow():

    @task()
    def get_data(sentence_length):
        conn, cursor = get_connect_db('product_db')
        product_data_list = []
        for table_name in TABLE_LIST:
            select_sql = f'''
                        SELECT * FROM {table_name}
                        WHERE product_state = True AND product_price > 50000 AND product_price < 3500000;
                        '''
            cursor.execute(select_sql)
            product_data_list += cursor.fetchall()

        df = pd.DataFrame(product_data_list)
        df['product_title'] = df['product_title'].str.lower()
        word_filter = df['product_title'].str.contains(DELETE_WORD, case=False)
        drop_index = df[word_filter].index
        filter_df = df.drop(drop_index)
        mask = pd.Series([True if len(i) > sentence_length else False for i in filter_df['product_info']])
        filter_df.reset_index(drop=True, inplace=True)
        filter_df = filter_df[mask].reset_index(drop=True)
        filter_dict = filter_df.to_dict('records')
        cursor.close()
        conn.close()

        return json.dumps(filter_dict, indent=4, sort_keys=True, default=str)
        
    @task()
    def preprocessing(filter_dict):
        
        filter_dict = json.loads(filter_dict)
        filter_df = pd.DataFrame(filter_dict)
        lst = [None] * len(filter_df)
        filter_df['product_model'] = lst

        lst_apple = ['max','iphone','아이폰','애플','se','apple','xe', 'xr']
        for i in lst_apple:
            mask = filter_df['product_title'].str.contains(i, case=False)
            filter_df.loc[mask,'product_model'] = "아이폰"

        iphone_df = filter_df[filter_df['product_model'] == '아이폰']
        samsung_df = filter_df[filter_df['product_model'] != '아이폰']
        
        lst_samsung = ['s10','s9','s8','s7','s6','s20','s21','s22','삼성',
                '겔','갤','노트','플립','폴드','폴더','gal',
                'fold','flip','플립','a']
        for i in lst_samsung:
            mask = samsung_df['product_title'].str.contains(i, case=False)
            samsung_df.loc[mask,'product_model'] = "갤럭시"
        
        samsung_df.dropna(inplace=True)
        filter_df = pd.concat([iphone_df, samsung_df])
        filter_df.reset_index(drop=True, inplace=True)

        for i in range(len(filter_df['product_title'])):
            filter_df.loc[i, 'product_title'] = re.sub('[^0-9a-zA-Zㄱ-힗]', ' ', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('[+]', '플러스', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('pro', '프로', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('max', '맥스', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('mini', '미니', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('flip', '플립', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('fold', '폴드', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_title'] = re.sub('ultra', '폴드', filter_df.loc[i, 'product_title'])
            filter_df.loc[i, 'product_info'] = re.sub('[^ㄱ-힗]', ' ', filter_df.loc[i, 'product_info'])

        remove_lst = ['s10','s9','s8','s7','s6','s20','s21','s22','삼성',
                '갤럭시','노트','플립','폴드','폴더','gal','fold','flip','플립',
                'a','max','iphone','아이폰','애플','se','apple','xe','미니','mini']

        for i in range(len(filter_df)):
            lst = filter_df.loc[i, 'product_info'].split()
            for txt in remove_lst:
                for j in range(len(lst) - 1, -1, -1):
                    if txt in lst[j]:
                        try:
                            del lst[j]
                        except:
                            pass
            filter_df.loc[i, 'product_info'] = ' '.join(lst)
        
        check_lst = [False] * len(filter_df)
        filter_df['check_processing'] = check_lst

        filter_dict = filter_df.to_dict('records')
        conn, cursor = get_connect_db('product_db')
        for data in filter_dict:
            try:
                insert_sql = '''
                        INSERT INTO preprocessing_table
                        VALUES(%(product_code)s,%(product_domain)s,%(product_title)s,%(product_price)s,
                        %(product_info)s,%(product_date)s,%(product_state)s,%(product_location)s,%(product_model)s,%(check_processing)s);
                        '''
                cursor.execute(insert_sql, data)
                conn.commit()
            except:
                continue
        cursor.close()
        conn.close()

    joongna_data = get_data(10)
    data_preprocessing = preprocessing(joongna_data)

preprocessing_flow = taskflow()