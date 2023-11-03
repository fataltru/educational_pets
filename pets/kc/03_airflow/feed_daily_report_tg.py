# скрипт для сборки отчета по ленте новостей. 
# В отчет включим следующие метрики: DAU, Просмотры, Лайки, CTR

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
def kryuchkov_feed_main_metric():
    
    #Выгрузка из БД таблицы ленты
    @task   
    def extract_feed():
            query = '''
                select toDate(time) as dt, 
                    count(distinct user_id) as dau,
                    countIf(action='view') as views,
                    countIf(action='like') as likes,
                    round(likes / views, 4) as ctr
                from simulator_20230920.feed_actions
                where toDate(time) between date_sub(day, 7, today()) and yesterday()
                group by toDate(time)        
            '''
            df = getch(query).df
            return df
        
        
    #Подготовка данных
    @task   
    def get_min_data(df):
        return pd.to_datetime(min(df['dt'])).date()
      

    @task   
    def transform_metrics_data(feed_df, owa):
        
        #развернем сводную таблицу (перенесем столбцы с метриками в построчный вид)
        feed_df_melt = pd.melt(
            feed_df,
            id_vars=['dt'],
            value_vars=['dau','views','likes','ctr'],
            var_name = 'metrics',
            value_name ='value'
       )
        #создадим основой ДФ с данными за вчерашний день
        yesterday_feed_df = feed_df_melt.query(f'dt == "{yesterday_dt}"').reset_index(drop=True)
        yesterday_feed_df.index = yesterday_feed_df.index + 1
        
        #рассчитаем процентный прирост метрик вчерашнего дня по отношению к позавчерашнему
        ratio_growth_yesterday = round(
            (
            feed_df_melt.query(f'dt == "{yesterday_dt}"').set_index('metrics')['value'] \
             / feed_df_melt.query(f'dt == "{two_day_ago}"').set_index('metrics')['value']
            )-1, 4
        )
        ratio_growth_yesterday.name = f'growth_ratio_tda'
        
        #рассчитаем процентный прирост метрик вчерашнего дня по отношению к данным недельной давности
        ratio_growth_one_week_ago = round(
            (
            feed_df_melt.query(f'dt == "{yesterday_dt}"').set_index('metrics')['value'] \
             / feed_df_melt.query(f'dt == "{owa}"').set_index('metrics')['value']
            )-1, 4
        )
        ratio_growth_one_week_ago.name = f'growth_ratio_owa'
        
        #объеденим полученные проценты прироста и ДФ за вчерашний день
        yesterday_feed_df = yesterday_feed_df.merge(ratio_growth_yesterday, how='left', left_on = 'metrics', right_index=True)
        yesterday_feed_df = yesterday_feed_df.merge(ratio_growth_one_week_ago, how='left', left_on = 'metrics', right_index=True)
        yesterday_feed_df = yesterday_feed_df.drop(columns='dt')
        
        return yesterday_feed_df

    #Подготовка сообщения
    @task   
    def create_metrics_msg(df, owa):         
        msg = f'Основные показатели работы ленты за вчерашний день, {yesterday_dt}\n\n'
        tab = '\t'*3
         
        #пройдемся по каждой метрики, и сформируем 1 строчку с необходимой разметкой 
        for i in range(0, len(df)):

            msg = msg + f'{i+1}. {df.iloc[i,0]} = '

            if df.iloc[i,0] == 'ctr':
                msg = msg + f'{df.iloc[i,1]:,.2%}\n'
            else:
                msg = msg + f'{df.iloc[i,1]:,.0f}\n'.replace(',', ' ')

            msg = msg + f'{tab}{df.iloc[i,2]:+.2%} к {two_day_ago}\n'
            msg = msg + f'{tab}{df.iloc[i,3]:+.2%} к {owa}\n\n'
        
        return msg    
    
    #PDF файла с недельными графиками
    @task   
    def create_metrics_chart(feed_df, owa):         
        plot_object = io.BytesIO()
        #создадим полотно для 3-х графиков
        fig, axs = plt.subplots(3,1, figsize = (9,12))
        fig.suptitle(f'''Рассматриваемый период метрик\nс {owa} по {yesterday_dt}\n''', fontsize=20)
        
        #для 3-х метрик построим 3-и графика. На каждом графике зададим вторую ось Y, для отображение CTR
        for i in range(0, 3):
            name_columns = feed_df.iloc[:,i+1].name
            axs[i].set_title(f'График {name_columns} и ctr', fontsize=15)
            color = 'tab:red'
            axs[i].set_xlabel('dt', fontsize=15)
            axs[i].set_ylabel(name_columns, color=color, fontsize=15)
            sns.lineplot(data = feed_df, x='dt', y = name_columns, color=color, ax=axs[i], label=name_columns)
            axs[i].tick_params(axis='y', labelcolor=color)
            axs[i].grid()

            axs0_2 = axs[i].twinx()  # instantiate a second axes that shares the same x-axis

            color = 'tab:blue'
            axs0_2.set_ylabel('ctr', color=color, fontsize=15)
            sns.lineplot(data = feed_df, x='dt', y = 'ctr', color=color, ax=axs0_2, label='ctr')
            axs0_2.tick_params(axis='y', labelcolor=color)

            # Размещение легенды друг под другом
            lines1, labels1 = axs[i].get_legend_handles_labels()
            lines2, labels2 = axs0_2.get_legend_handles_labels()
            axs[i].legend(lines1 + lines2, labels1 + labels2, loc='best', fontsize=12, framealpha=0)
            axs0_2.get_legend().remove()   
            fig.tight_layout()  # otherwise the right y-label is slightly clipped
        #plt.show()
        plt.savefig(plot_object, format='pdf')
        plot_object.seek(0)
        plot_object.name = f'Краткий отчет ленты за {yesterday_dt}.pdf'
        plt.close()        
        return plot_object     

    @task   
    def load_data(msg, plot_object): 
        bot.sendMessage(chat_id=group_id, text=msg)
        bot.sendDocument(chat_id=group_id, document=plot_object)
    
        
    #Получение данных    
    feed_df = extract_feed()
    
    #установим даты 
    yesterday_dt = pd.to_datetime(datetime.now().date() - timedelta(days= 1)).date() #Дата отчета
    two_day_ago = pd.to_datetime(datetime.now().date() - timedelta(days= 2)).date() #Дата за день до отчета
    one_week_ago = get_min_data(feed_df) #Смещение на 1 неделю назад, относительно даты отчета
    
    
    #преобразование данных
    yesterday_feed_df = transform_metrics_data(feed_df, one_week_ago)
    msg = create_metrics_msg(yesterday_feed_df, one_week_ago)
    plot_object = create_metrics_chart(feed_df, one_week_ago)
    
    #отправка сообщения
    load_data(msg, plot_object)
    
#тело
kryuchkov_feed_main_metric = kryuchkov_feed_main_metric()

