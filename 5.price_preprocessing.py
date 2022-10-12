import pandas as pd
from sqlalchemy import create_engine
import pymysql
from pymysql import Error
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

def get_connect_db(db_name):
    conn = pymysql.connect(host = '54.64.211.139',
                     database = f'{db_name}',
                     user='jp',
                     password='multi1234!')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor

def price_preprocessing():
    conn, cursor = get_connect_db('product_db')
    sql = '''
    SELECT * FROM add_model_table;
    '''
    cursor.execute(sql)
    product_data_list = cursor.fetchall()
    df = pd.DataFrame(product_data_list)
    models = list(set(df['phone_model_name']))
    result = None
    for model in models:
        temp = df.loc[df['phone_model_name'] == model]
        new = temp.loc[temp['product_price'].between(temp['product_price'].quantile(q=0.05),temp['product_price'].quantile(q=0.95))]
        result = pd.concat([result,new], ignore_index=True )
    db_connection_str = 'mysql+pymysql://jp:multi1234!@54.64.211.139:3306/product_db'
    db_connection = create_engine(db_connection_str, encoding='utf-8')
    conn = db_connection.connect()
    result.to_sql(name='add_model_table_quantile', con=db_connection, if_exists='replace', index=False)
    cursor.close()
    conn.close()

KST = pendulum.timezone("Asia/Seoul")
default_args = {
    "start_date": datetime(2022, 7, 24, tzinfo=KST)
}

with DAG(
    dag_id="5.price_preprocessing",
    schedule_interval= "0 17 * * *",
    default_args=default_args,
    tags=["price_preprocessing", "pipeline"],
    catchup=False) as dag:
    
    price_pre = PythonOperator(
        task_id="5_price_5",
        python_callable=price_preprocessing
    )

    price_pre