# coding=utf-8
# Из таблицы feed_actions для каждого юзера посчитать число просмотров и лайков контента. 
# В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. 
# Объединяем две таблицы в одну, и считаем выше указанные метрики в срезах пола, возраста и операционной системы. 
# Полученные сразу записываем в одну таблицу в БД. Задача выполняется ежесуточно в 11:00

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#класс получения ДФ из БД simulator_20230920
class getch:
    def __init__(self, query, db=''):
        self.connection = {
            'host': '',
            'password': '',
            'user': '',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)
            
            
#Установка параметров подключения к тестовой БД            
connection_test_db = {
            'host': '',
            'password': '',
            'user': '',
            'database': ''
}

#Установка параметров для DAG 
default_args = {
    'owner': 'ni-krjuchkov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2023, 10, 5), # Дата начала выполнения DAG
}

schedule_interval = '0 11 * * *' # cron-выражение

#получение сводной таблицы (среза)
def get_pivot(df, dimension_value = 'os'):
    pivot = df.pivot_table(
                values=['messages_sent', 'users_sent', 'messages_received', 'users_received', 'views', 'likes'], 
                index=[dimension_value, 'event_date'], 
                aggfunc="sum"
            ).reset_index().rename(columns={dimension_value:'dimension_value'}).assign(dimension=dimension_value)
    return pivot


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kryuchkov_etl_main_metric():
    
    #Выгрузка из БД таблицы ленты
    @task   
    def extract_feed():
        query = '''
            select user_id as user_id_glob, os, age, if(gender = 1, 'male', 'female') as gender, 
            countIf(action='like') as likes,
            countIf(action='view') as views
            from simulator_20230920.feed_actions
            where toDate(time) = yesterday()
            group by user_id, os, age, gender
            
        '''
        df_feed = getch(query)
        return df_feed.df
    
    #Выгрузка из БД таблицы сообщений
    @task       
    def extract_msg():
        query = '''
            with sent_db as (
                select user_id as user_id_glob, 
                    os, age, if(gender = 1, 'male', 'female') as gender,
                    count() as messages_sent,
                    count(distinct receiver_id) as users_sent
                from simulator_20230920.message_actions
                where toDate(time) = yesterday()
                group by user_id, os, age, gender
            ), 

            receive_db as (
                select receiver_id as user_id_glob,
                    count() as messages_received,
                    count(distinct user_id) as users_received
                from simulator_20230920.message_actions
                where toDate(time) = yesterday()
                group by receiver_id

            )

            select *
            from sent_db as sdb
            left join receive_db as rdb
            using(user_id_glob)
        '''
        df_msg = getch(query)
        return df_msg.df
    
    #Объединение таблиц   
    @task       
    def transform_merge_db(df_msg, df_feed):
        df_msg_feed = df_msg.merge(df_feed, on='user_id_glob', how='outer') #объеденяем два ДФ

        #заполняем пустые значения в столбце os,age,gender ДФ ленты из данных мессенджера
        _ = [df_msg_feed[f'{x}_y'].fillna(df_msg_feed[f'{x}_x'], inplace=True) for x in ['os', 'age', 'gender']] 
        df_msg_feed.columns = df_msg_feed.columns.str.replace('_y', '')
        df_msg_feed = df_msg_feed.drop(columns={'os_x', 'age_x', 'gender_x'}) #удаляем дублирующие колонки которые пришли с ДФ мессенджера
        df_msg_feed = df_msg_feed.fillna(0) #заполняем нулями NaN (на пример количество сообщений у пользователей которые пользовались только лентой)
        df_msg_feed['event_date'] = datetime.now().date() - timedelta(days= 1)
        return df_msg_feed
    
    #Получение среза по ОС
    @task       
    def transform_os(df):
        return get_pivot(df, 'os')
    
    #Получение среза по гендеру
    @task       
    def transform_gender(df):
        return get_pivot(df, 'gender')  
    
    #Получение среза по возрасту
    @task
    def transform_age(df):
        return get_pivot(df, 'age') 
    
    #Объединение срезов
    @task     
    def transform_concat_df(slice_os, slice_gender, slice_age):
        concat_df = pd.concat([slice_os, slice_gender, slice_age], axis=0).astype(
                                                                       {'event_date' : 'datetime64[ns]',
                                                                        'dimension' : 'str',
                                                                        'dimension_value' : 'str',
                                                                        'views' : 'int32',
                                                                        'likes' : 'int32',
                                                                        'messages_received' : 'int32',
                                                                        'messages_sent' : 'int32',
                                                                        'users_received' : 'int32',
                                                                        'users_sent' : 'int32'}    
        )
        concat_df = concat_df[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']].reset_index(drop=True)
        return concat_df
    
    #Загрузка в БД
    @task
    def load_db(slice_df):     
        create_query = '''
        CREATE TABLE IF NOT EXISTS test.ni_krjuchkov
        (
            event_date Date,
            dimension String,
            dimension_value String,
            views Int32,
            likes Int32,
            messages_received Int32,
            messages_sent Int32,
            users_received Int32,
            users_sent Int32
        )
        ENGINE = MergeTree()
        ORDER BY event_date
        '''
        ph.execute(query=create_query, connection=connection_test_db)
        ph.to_clickhouse(df=slice_df, table='ni_krjuchkov', index=False, connection=connection_test_db)
    
    
    df_feed = extract_feed()
    df_msg = extract_msg()
    df_common = transform_merge_db(df_msg, df_feed)
    slice_os = transform_os(df_common)
    slice_gender = transform_gender(df_common)
    slice_age = transform_age(df_common)
    slice_all = transform_concat_df(slice_os, slice_gender, slice_age)
    load_db(slice_all)
    
    
#тело

kryuchkov_etl_main_metric = kryuchkov_etl_main_metric() 
        
    