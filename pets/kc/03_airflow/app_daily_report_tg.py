# скрипт для сборки отчета по приложению в целом (лента + чат). 
# Отчет будет состоять из трех частей:
# 1. текст с информацией о значениях ключевых метрик за предыдущий день
# 2. 4 группы графиков
# 3. Накопительный .csv файл с информацией по всем метрикам в различных срезах


#импорт библиотек
import io
import requests
from datetime import datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandahouse as ph
import telegram
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#класс для получения данных из БД
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
                
#настройка бота
bot_token = ''
bot = telegram.Bot(token=bot_token)
group_id = 

#установка горизонта выгрузки данных
horizon = 31

#установка параметров DAG по умолчанию
default_args = {
    'owner': 'ni-krjuchkov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2023, 10, 5), # Дата начала выполнения DAG
}

schedule_interval = '0 11 * * *' # cron-выражение

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kryuchkov_app_metric_report():
    
    # @task   
    # def get_min_data(df):
    #     return pd.to_datetime(min(df['dt'])).date() 
    
    #Выгрузка из БД данных о работе приложения в целом
    @task   
    def extract_general_app_info():
        query = f'''
                with general_table as 
                (
                    select toDate(time) as dt, 
                        user_id,
                        count(action) as action_cnt
                    from simulator_20230920.feed_actions
                    where toDate(time) between date_sub(day, {horizon}, today()) and yesterday()
                    group by toDate(time), user_id

                    union all

                    select toDate(time) as dt, 
                        user_id,
                        count(user_id) as action_cnt
                    from simulator_20230920.message_actions
                    where toDate(time) between date_sub(day, {horizon}, today()) and yesterday()
                    group by toDate(time), user_id
                )

                select 
                    'app' as service,
                    'total' as dimension,
                    'main' as dimension_value,
                    dt, 
                    count(distinct user_id) as dau,
                    sum(action_cnt) as actions_cnt,
                    cast(round(actions_cnt / dau, 0) as int) as apu
                from general_table
                group by dt        
            '''
        df = getch(query).df
        return df
    
    #Выгрузка метрик за все время существования приложения (лента+чат)
    @task
    def extract_total_metrics():
        query = f'''
                with general_table as 
                (
                    select user_id,
                        count(action) as action_cnt
                    from simulator_20230920.feed_actions
                    where toDate(time) <= yesterday()
                    group by user_id

                    union all

                    select user_id,
                        count(user_id) as action_cnt
                    from simulator_20230920.message_actions
                    where toDate(time) <= yesterday()
                    group by user_id
                )

                select 
                    'app' as service,
                    'total' as dimension,
                    'main' as dimension_value,
                    count(distinct user_id) as total_users,
                    sum(action_cnt) as total_actions_cnt,
                    cast(round(total_actions_cnt / total_users, 0) as int) as apu
                from general_table       
            '''
        df = getch(query).df
        return df

    #Выгрузка из БД данных о работе ленты
    @task   
    def extract_feed_info():
        query = f'''
            with raw_week_feed_table as (
                select *
                from simulator_20230920.feed_actions
                where toDate(time) between date_sub(day, {horizon}, today()) and yesterday()
            ),

            top_five_country as (
                select country
                from raw_week_feed_table
                group by country
                order by count(action) desc
                limit 5
            ),

            union_data_feed as (
                select 
                    'feed' as service,
                    'total' as dimension,
                    'main' as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr,
                    count(distinct post_id) as active_post_cnt,
                    likes + views as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu,
                    cast(round(actions_cnt / active_post_cnt, 0) as int) as app
                from raw_week_feed_table
                group by service, dimension, dimension_value, toDate(time)

                union all

                select 
                    'feed' as service,
                    'source' as dimension,
                    source as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr,
                    count(distinct post_id) as active_post_cnt,
                    likes + views as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu,
                    cast(round(actions_cnt / active_post_cnt, 0) as int) as app
                from raw_week_feed_table
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'feed' as service,
                    'os' as dimension,
                    os as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr,
                    count(distinct post_id) as active_post_cnt,
                    likes + views as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu,
                    cast(round(actions_cnt / active_post_cnt, 0) as int) as app
                from raw_week_feed_table
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'feed' as service,
                    'country' as dimension,
                    country as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr,
                    count(distinct post_id) as active_post_cnt,
                    likes + views as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu,
                    cast(round(actions_cnt / active_post_cnt, 0) as int) as app
                from raw_week_feed_table
                where country in (select * from top_five_country)
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'feed' as service,
                    'country' as dimension,
                    'other' as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr,
                    count(distinct post_id) as active_post_cnt,
                    likes + views as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu,
                    cast(round(actions_cnt / active_post_cnt, 0) as int) as app
                from raw_week_feed_table
                where country not in (select * from top_five_country)
                group by service, dimension, dimension_value, toDate(time)
            )

            select *
            from union_data_feed
            order by dt, service, dimension, dimension_value       
            '''
        df = getch(query).df
        return df
    
    #Выгрузка из БД данных о новых пользователях в ленте
    @task   
    def extract_feed_new_users():
        query = f'''
            select dt, count(user_id) as new_users_cnt
            from(
                select user_id, toDate(time) as dt, row_number() over(partition by user_id order by toDate(time) asc) as num
                from simulator_20230920.feed_actions
                group by user_id, toDate(time)
            ) as t1
            where num = 1 and dt between date_sub(day, {horizon}, today()) and yesterday()
            group by dt
            '''
        df = getch(query).df
        return df
    
    #Выгрузка из БД данных о новых постах в ленте
    @task   
    def extract_feed_new_posts():
        query = f'''
            select dt, count(post_id) as new_posts_cnt
            from(
                select post_id, toDate(time) as dt, row_number() over(partition by post_id order by toDate(time) asc) as num
                from simulator_20230920.feed_actions
                group by post_id, toDate(time)
            ) as t1
            where num = 1 and dt between date_sub(day, {horizon}, today()) and yesterday()
            group by dt

            '''
        df = getch(query).df
        return df
    
    #Выгрузка из БД данных о работе чата
    @task   
    def extract_message_info():
        query = f'''
            with raw_week_message_table as (
                select *
                from simulator_20230920.message_actions
                where toDate(time) between date_sub(day, {horizon}, today()) and yesterday()
            ),

            top_five_country as (
                select country
                from raw_week_message_table
                group by country
                order by count(user_id) desc
                limit 5
            ),

            union_data_message as (
                select 
                    'message' as service,
                    'total' as dimension,
                    'main' as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    count(user_id) as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu
                from raw_week_message_table
                group by service, dimension, dimension_value, toDate(time)

                union all

                select 
                    'message' as service,
                    'source' as dimension,
                    source as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    count(user_id) as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu
                from raw_week_message_table
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'message' as service,
                    'os' as dimension,
                    os as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    count(user_id) as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu
                from raw_week_message_table
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'message' as service,
                    'country' as dimension,
                    country as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    count(user_id) as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu
                from raw_week_message_table
                where country in (select * from top_five_country)
                group by service, dimension, dimension_value, toDate(time)

                union all
                select 
                    'message' as service,
                    'country' as dimension,
                    'other' as dimension_value,
                    toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    count(user_id) as actions_cnt,
                    cast(round(actions_cnt / dau,0) as int) as apu
                from raw_week_message_table
                where country not in (select * from top_five_country)
                group by service, dimension, dimension_value, toDate(time)
            )

            select *
            from union_data_message
            order by dt, service, dimension, dimension_value       
            '''
        df = getch(query).df
        return df
    
    #Выгрузка из БД данных о новых пользователях чата
    @task   
    def extract_message_new_users():
        query = f'''
            select dt, count(user_id) as new_users_cnt
            from(
                select user_id, toDate(time) as dt, row_number() over(partition by user_id order by toDate(time) asc) as num
                from simulator_20230920.message_actions
                group by user_id, toDate(time)
            ) as t1
            where num = 1 and dt between date_sub(day, {horizon}, today()) and yesterday()
            group by dt
            '''
        df = getch(query).df
        return df
    
    #преобразуем данные для отправки сообщения
    @task   
    def transform_metrics_data(df, column_vars, columns_list, owa):
        #развернем сводную таблицу (перенесем столбцы с метриками в построчный вид)
        column_vars = list(column_vars)
        df_melt = pd.melt(
            df,
            id_vars=list(set(['dt'] + column_vars)),
            value_vars=columns_list,
            var_name = 'metrics',
            value_name ='value'
       )
        #создадим основой ДФ с данными за вчерашний день
        yesterday_df = df_melt.query(f'dt == "{yesterday_dt}"').reset_index(drop=True)
        #yesterday_df.index = yesterday_df.index + 1

        #рассчитаем процентный прирост метрик вчерашнего дня по отношению к позавчерашнему
        ratio_growth_yesterday = round(
            (
            df_melt.query(f'dt == "{yesterday_dt}"').set_index(list(column_vars + ['metrics']))['value'] \
             / df_melt.query(f'dt == "{two_day_ago}"').set_index(list(column_vars + ['metrics']))['value']
            )-1, 4
        )
        ratio_growth_yesterday.name = f'growth_ratio_tda'

        #рассчитаем процентный прирост метрик вчерашнего дня по отношению к данным недельной давности
        ratio_growth_one_week_ago = round(
            (
            df_melt.query(f'dt == "{yesterday_dt}"').set_index(list(column_vars + ['metrics']))['value'] \
             / df_melt.query(f'dt == "{owa}"').set_index(list(column_vars + ['metrics']))['value']
            )-1, 4
        )
        ratio_growth_one_week_ago.name = f'growth_ratio_owa'

        #объеденим полученные проценты прироста и ДФ за вчерашний день
        yesterday_df = yesterday_df.merge(ratio_growth_yesterday, how='left', left_on = list(column_vars + ['metrics']), right_index=True)
        yesterday_df = yesterday_df.merge(ratio_growth_one_week_ago, how='left', left_on = list(column_vars + ['metrics']), right_index=True)
        yesterday_df = yesterday_df.drop(columns='dt')

        return yesterday_df
    
    #создание сообщения с показателями всех метрик для отправки
    @task
    def create_metrics_msg(df, total_metrics): 
        
        #функция для обработки каждого отдельного блока сообщений
        def msg_part(df_part, counter):
            msg_part = ''
            df_part = df_part.reset_index()
            for i in range(0, len(df_part)):
                msg_part = msg_part + f'{counter+1}. {df_part.loc[i,"metrics"]} = '

                if df_part.loc[i,"metrics"] == 'ctr':
                    msg_part = msg_part + f'{df_part.loc[i,"value"]:,.2%} '
                else:
                    msg_part = msg_part + f'{df_part.loc[i,"value"]:,.0f} '.replace(',', ' ')

                msg_part = msg_part + f'({df_part.loc[i,"growth_ratio_tda"]:+.2%}, '
                msg_part = msg_part + f'{df_part.loc[i,"growth_ratio_owa"]:+.2%})\n'
                counter += 1
            return counter, msg_part

        glob_counter = 0
        line = '-'*50
        tab = '\t'*3
        
        msg = f'''За все время было привлечено {total_metrics.loc[0,'total_users']:,.0f} пользователей
а так же было совершено {total_metrics.loc[0,'total_actions_cnt']:,.0f} действий'''.replace(',', ' ')

        msg = msg + f'''

Отчет за вчерашний день, {yesterday_dt} в формате:
"метрика" = "значение" за отчетную дату ("% прироста" к {two_day_ago},"% прироста" к {one_week_ago})   

Метрики приложения:
{line}
'''
        glob_counter, temp_msg = msg_part(df['app_metric_df'], glob_counter)
        msg = msg + temp_msg

        msg = msg + f'''
Метрики ленты:
{line}
'''
        glob_counter, temp_msg = msg_part(df['feed_metric_df'].query('dimension == "total"'), glob_counter)
        msg = msg + temp_msg

        glob_counter, temp_msg = msg_part(df['new_users_feed_metric'], glob_counter)
        msg = msg + temp_msg

        glob_counter, temp_msg = msg_part(df['new_posts_metric'], glob_counter)
        msg = msg + temp_msg

        msg = msg + f'''
Метрики чата:
{line}
'''
        glob_counter, temp_msg = msg_part(df['message_metric_df'].query('dimension == "total"'), glob_counter)
        msg = msg + temp_msg

        glob_counter, temp_msg = msg_part(df['new_users_message_metric'], glob_counter)
        msg = msg + temp_msg

        return msg           
    
    #функция для получения объедененного (приложения в целом, ленты и чата) ДФ с метриками (для построения графика)
    @task
    def get_concat_df(app_df, feed_df, message_df):
        return pd.concat([app_df, feed_df, message_df], ignore_index=True, sort=False, axis=0).sort_values(by = 'dimension', ascending=False)
    
    #создание csv файла с данными за указанный горизонт
    @task
    def create_csv_file(df):
        csv_obj = io.BytesIO()
        df.to_csv(csv_obj, decimal=',')
        csv_obj.seek(0)
        csv_obj.name = f'Отчет по приложению за {horizon_day_ago} - {yesterday_dt}.csv'
        return csv_obj
    
    #функция для построения графиков (возврощает объект)
    @task
    def plot_graph(df=pd.DataFrame(), query_param='', x='dt', y=[], hue='dimension_value', service_name='', additional_df = [], file_name='unknown'):
        
        plot_object = io.BytesIO()
        if len(query_param) > 0:
            df = df.query(query_param)
        else:
            pass
        
        plot_len = (len(y) * len(df['dimension'].unique())) + len(additional_df) #расчет количество графиков
        fig, axs = plt.subplots(plot_len, 1, figsize=(12, (plot_len) * 4))
        fig.suptitle(f'''Информация по {service_name}\nс {horizon_day_ago} по {yesterday_dt}''', fontsize=16, y=0.997)
        plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
        #fig.tight_layout(h_pad=6)   

        for top_count, dimension in enumerate(df['dimension'].unique()): #проходимся по всем измерениям    
            for count, value in enumerate(y, start = top_count * len(y)): #проходимся по каждому срезу в измерении
                sns.lineplot(data = df.query('dimension == @dimension'), x='dt', y=value, hue=hue, ax = axs[count], alpha=0.7)
                axs[count].grid()
                axs[count].tick_params(axis="x", labelrotation=20)
                axs[count].set_title(f'Срез: {dimension}, метрика {value}')
                axs[count].set_xlabel('')

        if len(additional_df) > 0: #проверяем дополнительные ДФ с одной метрикой
            sub_k = plot_len - len(additional_df)
            for count, value in enumerate(additional_df, start = plot_len - len(additional_df)):
                sns.lineplot(data = additional_df[count-sub_k].set_index('dt'), ax = axs[count], alpha=0.7)
                axs[count].grid()
                axs[count].tick_params(axis="x", labelrotation=20)
                axs[count].set_title(f'Метрика {additional_df[count-sub_k].iloc[:,1].name}')
                axs[count].set_xlabel('')
        else:
            pass
        # plt.show()
        fig.tight_layout()
        plt.savefig(plot_object, format='pdf')
        plot_object.seek(0)
        plot_object.name = file_name
        plt.close()
        return plot_object
      
    #отправка данных
    @task   
    def load_data(msg, csv_obj, graph_msg_group): 
        bot.sendMessage(chat_id=group_id, text=msg)
        bot.sendDocument(chat_id=group_id, document=csv_obj)
        bot.sendMediaGroup(chat_id=group_id, media=[telegram.InputMediaDocument(file_name) for file_name in graph_msg_group])
    
    #получение данных
    app_df = extract_general_app_info()
    feed_df = extract_feed_info()
    new_users_feed = extract_feed_new_users()
    new_posts = extract_feed_new_posts()
    message_df = extract_message_info()
    new_users_message = extract_message_new_users()
    total_metrics = extract_total_metrics()
    concat_df = get_concat_df(app_df, feed_df, message_df)
    
    #установка необходимых дат
    yesterday_dt = pd.to_datetime(datetime.now().date() - timedelta(days= 1)).date() #Дата отчета
    two_day_ago = pd.to_datetime(datetime.now().date() - timedelta(days= 2)).date() #Дата за день до отчета
    one_week_ago = pd.to_datetime(datetime.now().date() - timedelta(days= 7)).date() #Смещение на 1 неделю назад, относительно даты отчета
    horizon_day_ago = pd.to_datetime(datetime.now().date() - timedelta(days=horizon)).date()
    
    #установим срезы, которые подадим в функцию transform_metrics_data
    app_columns_value = ['dau', 'actions_cnt', 'apu']
    additional_columns_vars = ['dimension', 'dimension_value']
    feed_columns_value = ['dau', 'views', 'likes', 'ctr', 'active_post_cnt', 'actions_cnt', 'apu', 'app']
    message_columns_value = ['dau', 'actions_cnt', 'apu']
    
    #получим словарь со всеми срезами для создания сообщения.
    metric_dict = {}
    metric_dict['app_metric_df'] = transform_metrics_data(app_df, additional_columns_vars, app_columns_value, one_week_ago)
    metric_dict['feed_metric_df']  = transform_metrics_data(feed_df, additional_columns_vars, feed_columns_value, one_week_ago)
    metric_dict['new_posts_metric'] = transform_metrics_data(new_posts, [], ['new_posts_cnt'], one_week_ago)
    metric_dict['new_users_feed_metric'] = transform_metrics_data(new_users_feed, [], ['new_users_cnt'], one_week_ago)
    metric_dict['message_metric_df'] = transform_metrics_data(message_df, additional_columns_vars, message_columns_value, one_week_ago)
    metric_dict['new_users_message_metric'] = transform_metrics_data(new_users_message, [], ['new_users_cnt'], one_week_ago)
    
    #создадим файл для отправки содержащий данные ленты, чата и приложения в целом 
    #(возможно использовать для дальнейшего анализа локально у получателя)
    csv_object = create_csv_file(concat_df)
    
    #получим все объекты содержащие необходимые графики
    feed_graph = plot_graph(
        concat_df, 
        query_param ='(dimension == "total") & (service == "feed")',
        x ='dt', 
        y = ['dau', 'actions_cnt', 'ctr', 'apu', 'active_post_cnt', 'app'], 
        service_name ='ленте', 
        additional_df = [new_users_feed, new_posts],
        file_name = f'Графики ленты за {yesterday_dt}.pdf'
    )
    
    feed_graph_slice = plot_graph(
        concat_df, 
        query_param = '(dimension != "total") & (service == "feed")',
        x ='dt', 
        y = ['dau', 'actions_cnt', 'ctr', 'apu'], 
        service_name ='ленте, в разрезе страны, ОС, источника',
        file_name = f'Графики ленты (разрез страны, ОС, источник) за {yesterday_dt}.pdf'
    )
    
    message_graph = plot_graph(
        concat_df, 
        query_param = '(service == "message")',
        x ='dt', 
        y =['dau', 'actions_cnt', 'apu'], 
        service_name ='чату', 
        additional_df = [new_users_message],
        file_name = f'Графики чата за {yesterday_dt}.pdf'
    )
    
    app_graph = plot_graph(
        concat_df, 
        query_param = '(dimension == "total")', 
        x ='dt', 
        y = ['dau', 'apu'], 
        hue ='service', 
        service_name ='приложению', 
        file_name = f'Графики приложения в целом за {yesterday_dt}.pdf'
    )
    
    #выполним отправку всего, что собрали выше
    load_data(create_metrics_msg(metric_dict, total_metrics), csv_object, [app_graph, feed_graph, feed_graph_slice, message_graph])

#тело
kryuchkov_app_metric_report = kryuchkov_app_metric_report()