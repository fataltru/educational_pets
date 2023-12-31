# Анализ оттока клиентов банка

## Описание и задача проекта

Требуется проанализировать клиентов регионального банка, найти, выделить и численно описать лояльные сегменты, дать рекомендации по удержанию клиентов из данных сегментов, а так же по привлечению новых клиентов попадающих под описание данных сегментов

## Краткий план действий
    1. Загрузка данных и подготовка их к исследованию
    2. Подготовка данных (анализ пропусков) и их кодирование
    3. Исследовательский анализ, поиск взаимосвязей между характеристиками (распределения, в т.ч. в разрезе оттока, портрета клиентов и их сравнение, корреляционный анализ).
    4. Проверка статистических гипотез.
    5. Промежуточной итог о том, какие показатели связаны с отсутствием оттока клиентов
    6. Сегментация на основе стратегических показателей
    7. Обрисуем выбранные нами сегменты, дадим характеристики данных сегментов и как следствие подведем итого, дадим рекомендации привлечению клиентов такого рода сегментов.

## Используемые инструменты
- Pandas, Numpy, phik, Matplotlib, Seaborn, scipy.stats (проверка гипотез)


## Общий вывод

Приведем портрет лояльных и отточных клиентов:
| показатель | значение лояльных | значение отточных | % превышения показателя лояльных над отточными клиентами|
|---|---|---|---|
| Медианный баланс | 320 707 | 775 349 | -141% |
| Не относится к vip клиентам | 7% | 14% | -96%|
| Менее половины таких клиентов не активны | 48% | 70% | -45% |
| Медианный рейтинг собственности | 3 | 4 | -33% |
| Медианная Расчетная заработная плата | 118 249 | 125 390 | -6% |
| Средний кредитный рейтинг | 845 | 863 | -2% |
| Медианное количество продуктов банка | 2 | 2 | 0% |
| Возраст | 40 | 39 | 2.5% |
| Наличие кредитной карты | 71% | 55% | 22% |
| Является ли женщиной | 53% | 35% | 34% |

Были проверены три гипотезы:

    1. О равенстве зарплат у лояльных и отточных клиентов. В результате теста мы отвергли нулевую гипотезу в пользу альтернативной (средние зарплаты не равны)
    2. О независимоти переменных лояльности и кредитной карты. В результате теста мы отвергли нулевую гипотезу в пользу альтернативной (между переменными есть взаимосвязь)
    3. О независимости переменных лояльности и города. В пезультате теста мы можем утверждать, что есть статистически значимая зависимость между городом и оттоком/лояльностью клиента

Так же были выделены 4 сегмента, на основании характеристик где уровень лояльности был выше среднего уровня лояльности по банку (81%)

    1. Баланс до 700 000 и в возрасте от 35 до 48, размер группы 1622 клиента, лояльность 99%, рейтинг 19
    2. Баллы собственности до 2 (включительно), кредитный рейтинг больше 920, размер группы 767 клиентов, лояльность 98%, рейтинг 21
    3. Активность в последний месяц отсутствует, кредитная карта имеется, размер группы 3319 клиентов, лояльность 90%, рейтинг 28
    4. Женщины старше 60, размер группы 917 клиентов, лояльность 95%, рейтинг 28.5