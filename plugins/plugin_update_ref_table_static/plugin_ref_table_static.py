import pandas as pd
import psycopg2
import clickhouse_connect
from datetime import datetime
import typing


class Update_ref_table_static:

    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict):
        """
        :param dict_postgre_cred: словарь с параметрами подключения к базе данных Postgre

        :param dict_click_cred: словарь с параметрами подключения к базе данных Clickhouse
        """
        # Коннект для выгрузки данных из Postgre
        self.psycopg_connect = psycopg2.connect(user=dict_postgre_cred['user'],
                                                password=dict_postgre_cred['password'],
                                                host=dict_postgre_cred['host'],
                                                port=dict_postgre_cred['port'],
                                                database=dict_postgre_cred['database'])

        # Клиент для инициализации работы с БД Clickhouse
        self.click_house_client = clickhouse_connect.get_client(host=dict_click_cred['host'],
                                                                port=dict_click_cred['port'],
                                                                username=dict_click_cred['user'],
                                                                password=dict_click_cred['password'],
                                                                database=dict_click_cred['database'])

    def get_variables(self, operation_name: str) -> dict:
        """
        :param operation_name: имя операции из БД

        :return: словарь параметров для переданной операции
        """
        with self.psycopg_connect.cursor() as cur:
            cur.execute("""SELECT variables_dag 
                           FROM bot.status_operation
                           WHERE operation_name = %s""",
                        (operation_name,))
            return cur.fetchone()[0]

    def finel_list_table(self, operation_name: str, list_name_table_sql: typing.Optional[list[str]] = None,
                         list_sql_scripts: typing.Optional[list[str]] = None,
                         list_name_table_func: typing.Optional[list[str]] = None) -> tuple | list:
        """
        Нужна для валидации обновляемых таблиц,
        чтобы обновить только нужные или все

        :param operation_name: имя операции из БД

        :param list_name_table_sql: список названий таблиц, которые обновляются из sql скриптов

        :param list_sql_scripts: список sql скриптов, чтобы отобрать нужные

        :param list_name_table_func: список таблиц, которые обновляются за счет данных собранных в функции

        :return: список или картеж с нужными таблицами и скрипатми(если передан параметр list_name_table_sql)
        """

        # Параметры передающиеся при триггере DAG
        variables = self.get_variables(operation_name)
        name_table_by_sql = variables['name_table_by_sql']
        name_table_by_run_func = variables['name_table_by_run_func']

        # Если фильтруем названия таблиц обновляемые sql скриптами
        if list_name_table_sql:
            if name_table_by_sql == 'all':
                return list_name_table_sql, list_sql_scripts
            else:
                indx_value = list_name_table_sql.index(name_table_by_sql)
                return [list_name_table_sql[indx_value]], [list_sql_scripts[indx_value]]
        # Если фильтруем названия таблиц обновляемые функцией(набор данных собирается в момент выполнения функции)
        elif list_name_table_func:
            if name_table_by_run_func == 'all':
                return list_name_table_func
            else:
                indx_value = list_name_table_func.index(name_table_by_run_func)
                return [list_name_table_func[indx_value]]


    def update_ref_date_table(self, name_clickhouse_table: str, **kwargs):
        """
        Обновление справочников месяцев и лет.
        Собираются исходя из переданных данных

        :param name_clickhouse_table: имя таблицы справочника в clickhouse

        :param kwargs: набор параметров запуска

        :return:
        """

        # Задаем русскую локаль, для русских названий месяцев
        import locale
        current_locale = locale.getlocale(locale.LC_TIME)
        locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

        # Параметры передающиеся при триггере DAG
        year_lower_border = kwargs["dag_run"].conf['year_lower_border']
        year_upper_border = kwargs["dag_run"].conf['year_upper_border']
        is_shown_lower_border = kwargs["dag_run"].conf['is_shown_lower_border']
        is_shown_upper_border = kwargs["dag_run"].conf['is_shown_upper_border']

        # В зависимости от обновляемой таблицы выполняем нужный кусок кода
        if name_clickhouse_table == 'ref_years':
            dict_for_df = {'code': [], 'value': []}
            count_year = 0
            for year in range(year_lower_border, year_upper_border + 1):
                count_year += 1
                dict_for_df['code'].append(count_year)
                dict_for_df['value'].append(year)
        else:
            dict_for_df = {'name_date': [], 'value': [], 'year': [],
                          'month': []}
            for year in range(year_lower_border, year_upper_border + 1):
                for month in range(1, 13):
                    dict_for_df['name_date'].append(datetime(year, month, 1).strftime('%Y-%b'))
                    dict_for_df['value'].append(datetime(year, month, 1).strftime('%Y-%m'))
                    dict_for_df['year'].append(year)
                    dict_for_df['month'].append(month)


        df = pd.DataFrame(dict_for_df)
        df['is_shown'] = df['value' if name_clickhouse_table == 'ref_years' else 'year'] \
                          .apply(lambda x: is_shown_lower_border < x < is_shown_upper_border)

        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {name_clickhouse_table}")
        self.click_house_client.insert_df(table=name_clickhouse_table, df=df)

        # Меняем локаль на первоначальную
        locale.setlocale(locale.LC_TIME, current_locale)


    def run_scripts(self, sql_script: str, name_clickhouse_table: str):
        """
        Обновление статичных справочников из sql скриптов

        :param sql_script: скрипт обновляемой таблицы

        :param name_clickhouse_table: нужная таблица в базе данных ClickHouse
        """
        df_pg = pd.read_sql(sql_script, con=self.psycopg_connect)
        df_pg.fillna('', inplace=True)

        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {name_clickhouse_table}")
        self.click_house_client.insert_df(table=name_clickhouse_table, df=df_pg)
