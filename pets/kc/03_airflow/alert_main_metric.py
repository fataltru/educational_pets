# скрипт для сборки информации об основных метриках (активность пользователей, лайки, просмотры, ctr, количество сообщений), 
# который в случае обнаружения аномального значения, отправляется сообщение в чат с информацией: метрика, ее значение, величина отклонения.

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
from sklearn.neighbors import LocalOutlierFactor
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
    'start_date': datetime(2023, 10, 15), # Дата начала выполнения DAG
}

schedule_interval = '*/15 * * * *' # cron-выражение

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kryuchkov_anomaly_check_lof_off():
    
    #Выгрузка из БД таблицы ленты
    @task   
    def extract_feed():
            query = '''
                select toStartOfFifteenMinutes(time) as fifteen_dt, 
                    formatDateTime(fifteen_dt, '%R') as hm,
                    formatDateTime(fifteen_dt, '%F') as dt,
                    uniq(user_id) as fmau,
                    countIf(action='like') as like_cnt,
                    countIf(action='view') as view_cnt,
                    round(like_cnt / view_cnt, 4) as ctr,
                    'feed' as service
                from simulator_20230920.feed_actions
                where  time >= yesterday() and time < toStartOfFifteenMinutes(now())
                group by toStartOfFifteenMinutes(time)
                order by dt, hm        
            '''
            df = getch(query).df
            return df

    #Выгрузка из БД данных о работе чата
    @task   
    def extract_message():
        query = '''
            select toStartOfFifteenMinutes(time) as fifteen_dt, 
                formatDateTime(fifteen_dt, '%R') as hm,
                formatDateTime(fifteen_dt, '%F') as dt,
                uniq(user_id) as fmau,
                count(user_id) as message_cnt,
                'message' as service
            from simulator_20230920.message_actions
            where  time >= yesterday() and time < toStartOfFifteenMinutes(now())
            group by toStartOfFifteenMinutes(time)
            order by dt, hm     
            '''
        df = getch(query).df
        return df
    #создание ДФ с пометкой аномалий в разрезе метрики
    @task   
    def anomaly_check(df: pd.DataFrame, window: int = 5, a: int = 3, n_neighbors: int = 10, metric: str = '',
                  threshold_nof: float = 2, threshold_value_ratio: float = 50, threshold_ci_ratio: float = 35) -> pd.DataFrame:
        
        if len(df) == 0 or len(metric) == 0:
            print('Необходимо передать данные датафрейма и исследуемой метрики')
            return None
        else:
            #рассчитаем доверительные интервалы
            df = df.copy()
            df = df[['service', 'fifteen_dt', metric]]
            # lof = LocalOutlierFactor(n_neighbors=n_neighbors)
            df['q1'] = df[metric].rolling(window, closed='left', center=False).quantile(.25) 
            df['q3'] = df[metric].rolling(window, closed='left', center=False).quantile(.75) 
            df['iqr'] = df['q3'] - df['q1']
            df['ci_low'] = df['q1'] - df['iqr'] * a
            df['ci_high'] = df['q3'] + df['iqr'] * a

            df['ci_low'] = df['ci_low'].ewm(span=window, adjust=False).mean()
            df['ci_high'] = df['ci_high'].ewm(span=window, adjust=False).mean()
            
            Блок LOF
            df['lof'] = lof.fit_predict(df[metric].values.reshape(-1,1))
            df['nof'] = lof.negative_outlier_factor_
            
            #рассчитаем процент отклонение полученного значения от доверительного интервала
            df['growth_above_ci'] = [
                ((x / max_v) - 1) * 100 if x > max_v 
                else ((x / min_v) - 1) * 100 if x < min_v
                else np.NaN
                for x, max_v, min_v in list(zip(df[metric], df['ci_high'], df['ci_low']))
            ]
            
            df['metric'] = metric
            df = df.rename(columns={metric:'value'})
            df['growth_opa'] = np.NaN
            df['growth_oda'] = np.NaN
            df['is_anomaly'] = False
            #рассчитаем отклонение от предыдущей 15-ти минутки и от значения, которое было сутки назад
            df['growth_opa'] = ((df['value'] / df.shift(1)['value']) - 1) * 100
            df['growth_oda'] = ((df['value'] / df.shift(96)['value']) - 1) * 100
            
            #расчет нижних границ отклонения, в зависимости от поданного верхнего значения (как один из вариантов)
            #на примере числа 100, если было 100 а стало 130, то прирост составил +30%
            #если было 130, а стало 100, то отлчие составило -23%, таким образом расчитываем нижнию границу отклонения
            low_tvr = ((1 / (1+threshold_value_ratio/100)) - 1) * 100
            low_tc = ((1 / (1+threshold_ci_ratio/100)) - 1) * 100
            
            # создадим ДФ содержащий все аномальные значения, а именно где:
            # 1. Превышения порога LOF (локального уровня выборосов) и отклонение от доверительного интвервала превышает допустимые 
            # 2. Отклонение от вчерашних показателей и отклонение от доверительного интвервала превышает допустимые 
            # 3. Отклонение от предыдущей 15-ти минутки и отклонение от доверительного интвервала превышает допустимые
            # 4. Если текущее значение метрики == 0
            anomaly_df = df.query(
                f'(abs(nof) >= {threshold_nof} & ((growth_above_ci >= {threshold_ci_ratio}) | (growth_above_ci <= {low_tc}))) \
                | ( \
                    ((growth_oda >= {threshold_value_ratio}) | (growth_oda <= {low_tvr})) \
                    & ((growth_above_ci >= {threshold_ci_ratio}) | (growth_above_ci <= {low_tc})) \
                  ) \
                | ( \
                    ((growth_opa >= {threshold_value_ratio}) | (growth_opa <= {low_tvr})) \
                    & ((growth_above_ci >= {threshold_ci_ratio}) | (growth_above_ci <= {low_tc})) \
                 ) \
               | ( \
                   value == 0 \
                 )'
            )['is_anomaly'].replace(False, True)
            
            #занесем всю полученную информацию об аномалиях в исходный ДФ
            df['is_anomaly'] = anomaly_df
            df['is_anomaly'] = df['is_anomaly'].fillna(False) 
            
            #в случае если последнее значение является аномалией вернем весь ДФ, в ином случае только последнюю запись
            if df.iloc[-1:]['is_anomaly'].bool() == True:
                return df
            else:
                return df[-1:]
            
    #функция для построения графиков (возврощает объект), для каждого сервиса функция вызывается отдельно
    @task   
    def plot_graph(dict_df, service_name=''):

        key_df_with_anomaly = []
        #соберем все ключи словаря, где последняя запись аномальное значение
        for _, key_value in enumerate(dict_df.keys()):
            if (dict_df[key_value].iloc[-1:]['is_anomaly'].bool() == True) and (service_name in key_value):
                key_df_with_anomaly.append(key_value)
            else:
                pass
        #в случае если ключей более 1, то строим графики через subplot
        if len(key_df_with_anomaly) > 1:
            plot_object = io.BytesIO()
            plot_len = (len(key_df_with_anomaly)) #расчет количество графиков
            fig, axs = plt.subplots(plot_len, 1, figsize=(12, (plot_len) * 4))
            fig.suptitle(f'''Обнаруженные аномалии в {service_name}''', fontsize=16, y=0.997)
            plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)

            for i, value in enumerate(key_df_with_anomaly):
                sns.lineplot(
                            data=dict_df[value], x = 'fifteen_dt', y='value', 
                            marker="X", markersize=10, markevery=list(dict_df[value].query('is_anomaly == True').index), mfc = 'r', 
                            label=dict_df[value]['metric'].unique()[0],
                            ax = axs[i]
                            )
                sns.lineplot(data=dict_df[value], x = 'fifteen_dt', y='ci_low', label='ci_low', ax = axs[i])
                sns.lineplot(data=dict_df[value], x='fifteen_dt', y='ci_high', label='ci_high', ax = axs[i])
                axs[i].grid()
                axs[i].set_title(f'Метрика {dict_df[value]["metric"].unique()[0]}')
                axs[i].set_xlabel('')
                
            fig.tight_layout() 
            plt.savefig(plot_object, format='pdf')
            plot_object.seek(0)
            plot_object.name = f'Сервис {service_name}. аномалии в {dict_df[value].iloc[-1:]["fifteen_dt"].dt.strftime("%d.%m.%Y %H-%M").values[0]}.pdf'
            plt.close()
            return plot_object
        #в ином случае строим 1 график
        elif len(key_df_with_anomaly) == 1:
            plot_object = io.BytesIO()
            plt.figure(figsize=(12, 4))
            key_df_with_anomaly = key_df_with_anomaly[0]
            plt.title(
                f'''Обнаруженные аномалии в {service_name}
Метрика {dict_df[key_df_with_anomaly]["metric"].unique()[0]}''', fontsize=16, y=0.995
            )
            sns.lineplot(
                        data=dict_df[key_df_with_anomaly], x = 'fifteen_dt', y='value', 
                        marker="X", markersize=10, markevery=list(dict_df[key_df_with_anomaly].query('is_anomaly == True').index), mfc = 'r', 
                        label=dict_df[key_df_with_anomaly]['metric'].unique()[0]
                        )
            sns.lineplot(data=dict_df[key_df_with_anomaly], x = 'fifteen_dt', y='ci_low', label='ci_low')
            sns.lineplot(data=dict_df[key_df_with_anomaly], x='fifteen_dt', y='ci_high', label='ci_high')
            plt.grid()
            plt.xlabel('')
            plt.tight_layout() 

            plt.savefig(plot_object, format='pdf')
            plot_object.seek(0)
            plot_object.name = f'Сервис {service_name}. аномалии в {dict_df[key_df_with_anomaly].iloc[-1:]["fifteen_dt"].dt.strftime("%d.%m.%Y %H-%M").values[0]}.pdf'
            plt.close()
            return plot_object
        #если аномалий нет, то возвращаем None
        else:
            return None
        
    #функция для формирования сообщений с аномалиями (все сервисы в рамках 1 сообщения)
    @task          
    def message_create(dict_df, sn=['']):   
        key_df_with_anomaly = []  
        msg=''
        #для каждого сервиса сформируем свой блок текста в рамках 1 сообщения 
        for global_count, service_name in enumerate(sn):
            key_df_with_anomaly = []  
            for _, key_value in enumerate(dict_df.keys()):
                if (dict_df[key_value].iloc[-1:]['is_anomaly'].bool() == True) and (service_name in key_value):
                    key_df_with_anomaly.append(key_value)
                else:
                    pass

            if len(key_df_with_anomaly) >= 1:
                if global_count >= 1 and len(msg)>0:
                    msg=msg+'\n\n'
                else:
                    pass
                line = '-'*50
                msg= msg + f'Аномалии по сервису {service_name}\n{line}'
                for i, value in enumerate(key_df_with_anomaly):
                    last_rec = dict_df[value][-1:]
                    msg = msg + f'''\nМетрика: {last_rec['metric'].values[0]}
    Текущее значение {last_rec['value'].values[0]}
    Отклонение от предшествующей 15-ти минутки: {np.round(last_rec['growth_opa'].values[0], 2)}%
    Отклонение от вчера: {np.round(last_rec['growth_oda'].values[0],2)}%'''
            else:
                pass
        if len(msg) > 0:
            msg_head = f'''Время: {dict_df[list(anomaly_dict.keys())[0]].iloc[-1:]["fifteen_dt"].dt.strftime("%d.%m.%Y %H-%M").values[0]}
Обнаружены аномальные значения\n
Дашборд контролируемых метрик:
https://superset.lab.karpov.courses/superset/dashboard/4400/\n\n'''
            return msg_head+msg
        else:
            return None     
        
    @task   
    def load_data(msg, graph_list): 
        if msg != None:
            bot.sendMessage(chat_id=group_id, text=msg)
            bot.sendMediaGroup(chat_id=group_id, media=[telegram.InputMediaDocument(file_name) for file_name in graph_list if file_name != None])
        else:
            return('Аномалий не обнаружено')
    #выгрузим данные
    df_feed = extract_feed()
    df_message = extract_message()
    #создадим словарь с ДФ по каждой интересующей нас метрики
    anomaly_dict = {}
    anomaly_dict['feed_fmau'] = anomaly_check(
        df = df_feed, window = 5, a = 3, n_neighbors = 10, metric = 'fmau', 
        threshold_nof = 2.5, threshold_value_ratio = 30, threshold_ci_ratio = 25
    )
    anomaly_dict['feed_like'] = anomaly_check(
        df = df_feed, window = 5, a = 3, n_neighbors = 10, metric = 'like_cnt',
        threshold_nof = 2, threshold_value_ratio = 30, threshold_ci_ratio = 25
    )
    anomaly_dict['feed_ctr'] = anomaly_check(
        df = df_feed, window = 3, a = 2, n_neighbors = 10, metric = 'ctr', 
        threshold_nof = 2, threshold_value_ratio = 10, threshold_ci_ratio = 7
    )
    anomaly_dict['feed_view'] = anomaly_check(
        df = df_feed, window = 5, a = 3, n_neighbors = 10, metric = 'view_cnt',
        threshold_nof = 2.5, threshold_value_ratio = 30, threshold_ci_ratio = 25
    )
    anomaly_dict['message_msg_cnt'] = anomaly_check(
        df = df_message, window = 5, a = 3, n_neighbors = 10, metric = 'message_cnt',
        threshold_nof = 2, threshold_value_ratio = 30, threshold_ci_ratio = 25
    )
    anomaly_dict['message_fmau'] = anomaly_check(
        df = df_message, window = 5, a = 3, n_neighbors = 10, metric = 'fmau',
         threshold_nof = 2, threshold_value_ratio = 30, threshold_ci_ratio = 25
    )
    
    feed_graph = plot_graph(anomaly_dict, 'feed')
    message_graph =  plot_graph(anomaly_dict, 'message')
    msg = message_create(anomaly_dict, ['feed', 'message'])
    load_data(msg, [feed_graph, message_graph])
    
#тело
kryuchkov_anomaly_check_lof_off = kryuchkov_anomaly_check_lof_off()