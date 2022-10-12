from airflow.decorators import dag, task
import pendulum
from datetime import datetime, timedelta
import pymysql
import json
import pandas as pd
import re

KST = pendulum.timezone("Asia/Seoul")
GALAXY_MODEL_LIST = ['s22울트라','s22플러스','s22','s21울트라','s21플러스','s21','s20울트라','s20플러스','s20','s10플러스','s105','s10e','s10',
                        '9플러스','s9','s8플러스','s8','플립3','플립5','플립','폴드3','폴드2','폴드','노트20울트라','노트20','노트10플러스','노트10',
                        '노트9','노트8','a53','a23','a32','a52s','a12','a42','a51','a21s','a31','퀀텀3','퀀텀2','퀀텀']
IPHONE_MODEL_LIST = ['13프로맥스','13프로','13미니','아이폰13','12프로맥스','12프로','12미니','아이폰12','11프로맥스','11프로',
                     '아이폰11','아이폰xr','se3','se2','se1','se','xs맥스','아이폰xs','아이폰x','8플러스','아이폰8','7플러스','아이폰7']

default_args = {
    "start_date": datetime(2022, 7, 20, tzinfo=KST)
}

def get_connect_db(db_name):
    conn = pymysql.connect(host = '54.64.211.139',
                     database = f'{db_name}',
                     user='jh',
                     password='multi1234!')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor

@dag(dag_id='4.classification_pipeline',
    default_args=default_args,
    schedule_interval='0 16 * * *',
    tags=['preprocessing', 'classification'],
    catchup=False)

def taskflow():
    @task()
    def get_data():
        conn, cursor = get_connect_db('product_db')
        select_sql = f'''
                        SELECT 
                            product_code,
                            product_title,
                            product_price,
                            product_date,
                            product_model
                        FROM 
                            preprocessing_table
                        WHERE 
                            check_processing = False;
                        '''
        cursor.execute(select_sql)
        result = cursor.fetchall()

        update_sql = f'''
                    UPDATE 
                        preprocessing_table
                    SET 
                        check_processing = True
                    WHERE 
                        check_processing = False;
                    '''
        cursor.execute(update_sql)
        conn.commit()
        
        conn.close()
        cursor.close()

        return json.dumps(result, indent=4, sort_keys=True, default=str)

    @task()
    def iphone_classification(result):

        filter_dict = json.loads(result)
        filter_df = pd.DataFrame(filter_dict)
        iphone_df = filter_df[filter_df['product_model'] == '아이폰']
        iphone_df.reset_index(drop=True, inplace=True)
        iphone_df.drop(['product_model'],axis=1, inplace=True)
        iphone_df['title'] = iphone_df['product_title'].str.replace(" ", "")
        lst = [None] * len(iphone_df)
        iphone_df['phone_model_name'] = lst

        for i in range(len(iphone_df)):
            for model_name in IPHONE_MODEL_LIST:
                if(len(iphone_df.loc[[i]][iphone_df['title'].str.contains(f'{model_name}', case=False)]) == 1):
                    if(model_name == '아이폰13' or model_name == '아이폰12' or model_name == '아이폰11' or model_name == '아이폰xr' or 
                    model_name == '아이폰xs' or model_name == '아이폰x' or model_name == '아이폰8' or model_name == '아이폰7'):
                        iphone_df.loc[i, 'phone_model_name'] = model_name
                        break
                    elif(model_name == 'se1' or model_name == 'se'):
                        model_name = '아이폰se1'
                        iphone_df.loc[i, 'phone_model_name'] = model_name
                        break
                    else:
                        model_name = '아이폰'+model_name
                        iphone_df.loc[i, 'phone_model_name'] = model_name
                        break
            else:
                continue
        iphone_df.drop(['title'],axis=1, inplace=True)
        iphone_df.dropna(inplace=True)
        iphone_df.reset_index(drop=True, inplace=True)
        iphone_dict = iphone_df.to_dict('records')

        return json.dumps(iphone_dict, indent=4, sort_keys=True, default=str)

    @task()
    def galaxy_classification(result):

        filter_dict = json.loads(result)
        filter_df = pd.DataFrame(filter_dict)
        galaxy_df = filter_df[filter_df['product_model'] == '갤럭시']
        galaxy_df.reset_index(drop=True, inplace=True)
        galaxy_df.drop(['product_model'],axis=1, inplace=True)
        galaxy_df['title'] = galaxy_df['product_title'].str.replace(" ", "")
        lst = [None] * len(galaxy_df)
        galaxy_df['phone_model_name'] = lst

        for i in range(len(galaxy_df)):
            for model_name in GALAXY_MODEL_LIST:
                if(len(galaxy_df.loc[[i]][galaxy_df['title'].str.contains(f'{model_name}', case=False)]) == 1):
                    if(model_name == '플립3' or model_name == '플립' or model_name == '폴드3' or model_name == '폴드2' or model_name == '폴드'):
                        model_name = '갤럭시 z'+model_name
                        galaxy_df.loc[i, 'phone_model_name'] = model_name
                        break
                    elif(model_name == '플립5'):
                        model_name = '갤럭시 z플립 5g'
                        galaxy_df.loc[i, 'phone_model_name'] = model_name
                        break
                    elif(model_name == 's105'):
                        model_name = '갤럭시 s10 5g'
                        galaxy_df.loc[i, 'phone_model_name'] = model_name
                        break
                    else:
                        model_name = '갤럭시 '+model_name
                        galaxy_df.loc[i, 'phone_model_name'] = model_name
                        break
            else:
                continue
        galaxy_df.drop(['title'],axis=1, inplace=True)
        galaxy_df.dropna(inplace=True)
        galaxy_df.reset_index(drop=True, inplace=True)
        galaxy_dict = galaxy_df.to_dict('records')

        return json.dumps(galaxy_dict, indent=4, sort_keys=True, default=str)

    @task()
    def insert_data(iphone_dict, galaxy_dict):
        iphone_dict = json.loads(iphone_dict)
        galaxy_dict = json.loads(galaxy_dict)

        iphone_df = pd.DataFrame(iphone_dict)
        galaxy_df = pd.DataFrame(galaxy_dict)

        concat_df = pd.concat([iphone_df,galaxy_df])
        update_list = concat_df['product_code'].values.tolist()
        
        filter_dict = concat_df.to_dict('records')
        conn, cursor = get_connect_db('product_db')
        for data in filter_dict:
            try:
                insert_sql = '''
                        INSERT INTO add_model_table
                        VALUES(%(product_code)s,%(product_title)s,%(product_price)s,
                        %(product_date)s,%(phone_model_name)s);
                        '''
                cursor.execute(insert_sql, data)
                conn.commit()
            except:
                continue
        
        cursor.close()
        conn.close()



    get_classification_data = get_data()
    classification_iphone = iphone_classification(get_classification_data) 
    classification_galaxy = galaxy_classification(get_classification_data)
    data_insert = insert_data(classification_iphone, classification_galaxy)

preprocessing_flow = taskflow()


