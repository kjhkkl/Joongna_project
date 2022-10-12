from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import pymysql
from pykospacing import Spacing
from konlpy.tag import Okt
import re
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
import pickle
from sklearn.model_selection import train_test_split
import keras
from tensorflow.keras import Input, Model
from keras.models import Sequential
from keras.layers import LSTM, Dropout, Dense, Activation, SimpleRNN, Concatenate
from keras.layers import Embedding, Flatten, Bidirectional, Attention
from keras.layers import BatchNormalization, TimeDistributed
from tensorflow.keras import optimizers
from collections import Counter
from keras.models import load_model
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import pendulum
import csv

KST = pendulum.timezone("Asia/Seoul")

def get_connect_db():
    conn = pymysql.connect(host = '54.64.211.139',
                     database = 'product_db',
                     user='jh',
                     password='multi1234!')
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor

default_args = {
    "start_date": datetime(2022, 7, 26, tzinfo=KST)
}

with DAG(
    dag_id='6.model_training_pipeline',
    default_args=default_args,
    schedule_interval='0 18 * * FRI',
    tags=['training', 'pipeline'],
    catchup=False) as dag:

    @task()
    def get_data():
        conn, cursor = get_connect_db()
        select_sql = '''
            SELECT 
	            add_model_table_quantile.product_title,
	            preprocessing_table.product_info,
	            add_model_table_quantile.product_price
            FROM 
	            add_model_table_quantile
            INNER JOIN 
	            preprocessing_table ON add_model_table_quantile.product_code = preprocessing_table.product_code;
            '''
        cursor.execute(select_sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return json.dumps(result, indent=4, sort_keys=True, default=str)

    @task()
    def create_dataset(result):

        training_data = json.loads(result)
        df = pd.DataFrame(training_data)

        X_train, X_test, y_train, y_test = train_test_split(df['product_title']+df['product_info'],(df['product_price']),
                                                    random_state=42,test_size=0.3)
        
        y_train.to_csv('/home/ubuntu/airflow/data/y_train.csv')

        X_train_data = []
        okt = Okt()
        for title in X_train:
            tokenized = okt.morphs(title, stem=True, norm=True)
            X_train_data.append(tokenized)
    
        X_test_data = []
        for title in X_test:
            tokenized = okt.morphs(title, stem=True, norm=True)
            X_test_data.append(tokenized)
        
        lst = []
        for i in X_train_data:
            for j in i:
                lst.append(j)
        dic = Counter(lst)

        with open('/home/ubuntu/airflow/data/dic.pickle', 'wb') as outputfile:
            pickle.dump(dic, outputfile)

        lst = []
        for i in range(len(df)):
            for j in okt.pos(df.loc[i,'product_info']):
                lst.append((j[0],j[1]))
            for j in okt.pos(df.loc[i,'product_title']):
                lst.append((j[0],j[1]))

        dic_pos=Counter(lst)

        pos = ['Adjective','Adverb','Alpha','Conjunction','Determiner','Eomi',
                'Exclamation','Foreign','Josa','KoreanParticle','Modifier',
                'Noun','Number','PreEomi','Suffix','Verb','VerbPrefix']
        stoppos = ['Foreign', 'Conjunction', 'Eomi', 'PreEomi',
                    'KoreanParticle', 'Determiner', 'VerbPrefix','Josa','Exclamation']

        n = 0
        for i in sorted(dic.items(),key=lambda x:x[1]):
            if i[1] > 30:
                break
            n+=1
    
        stopwords = [i[0][0] for i in dic_pos.items() if i[0][1] in stoppos]
        stopwords += [i[0] for i in sorted(dic.items(),key=lambda x:x[1])[:n]]

        with open('/home/ubuntu/airflow/data/stopwords.pickle', 'wb') as f:
            pickle.dump(stopwords, f, pickle.HIGHEST_PROTOCOL)
    
        X_train_data = [[j for j in i if j not in stopwords] for i in X_train_data]
        X_test_data = [[j for j in i if j not in stopwords] for i in X_test_data]

        X_train_new = []
        for words in X_train_data:
            sentence = " ".join(words)
            X_train_new.append(sentence)
        X_test_new = []
        for words in X_test_data:
            sentence = " ".join(words)
            X_test_new.append(sentence)
    
        tokenizer = Tokenizer(num_words=10657, oov_token="<oov>")
        tokenizer.fit_on_texts(X_train_new)

        max_len = 150

        X = tokenizer.texts_to_sequences(X_train_new)
        X_train_padded = pad_sequences(X, max_len, truncating='post')
        np.save('/home/ubuntu/airflow/data/X_train_padded', X_train_padded)

        X = tokenizer.texts_to_sequences(X_test_new)
        X_test_padded = pad_sequences(X, max_len, truncating='post')
        np.save('/home/ubuntu/airflow/data/X_test_padded', X_test_padded)

    get_model_data = get_data()
    create_model_dataset = create_dataset(get_model_data)

    model_training = BashOperator(
        task_id="training_model",
        bash_command='python3 /home/ubuntu/airflow/dags/train_model.py'
    )

    create_model_dataset >> model_training





