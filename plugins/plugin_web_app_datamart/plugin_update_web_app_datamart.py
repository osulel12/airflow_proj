import clickhouse_connect
import pandas as pd
from datetime import datetime
import logging
import psycopg2


class Update_web_app_datamart:
    """
    Класс основной логики обновления витрины годовых данных веб приложения
    """
    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict):
        """
        :param dict_click_cred: словарь с подключением к ClickHouse
        :param dict_postgre_cred: словарь с подключением к PostgreSql
        """
        # Подключение к базе ClickHouse
        self.click_house_client = clickhouse_connect.get_client(host=dict_click_cred['host'],
                                                                port=dict_click_cred['port'],
                                                                username=dict_click_cred['user'],
                                                                password=dict_click_cred['password'],
                                                                database=dict_click_cred['database'])
        # Подключение к базе PostgreSql
        self.sqlalchemy_con = psycopg2.connect(user=dict_postgre_cred['user'],
                                               password=dict_postgre_cred['password'],
                                               host=dict_postgre_cred['host'],
                                               port=dict_postgre_cred['port'],
                                               database=dict_postgre_cred['database'])

    def get_ref_dict(self, sql_query: str, col_key: str, col_value: str, **kwargs) -> dict:
        """
        :param sql_query: sql запрос
        :param col_key: столбец, который будет ключом в словаре
        :param col_value: столбец, который будет значение в словаре
        :param kwargs: дополнительные параметры
        :return: словарь созданный из справочника
        """
        df = self.click_house_client.query_df(sql_query)
        return {k: v for k, v in zip(df[col_key].tolist(), df[col_value].tolist())}

    def truncate_median_table(self, table_name: str, **kwargs):
        """
        Очистка промежуточной таблицы обновления месячных данных
        :param table_name: имя очищаемой таблицы
        :param kwargs: дополнительные параметры
        """
        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")

    @staticmethod
    def return_value(x, dct, tp=None) -> str | int:
        """
        Используется в конструкции apply для возвращения значений
        из словаря или замены их в случае отсутсвия в словаре
        :param x: передаваемое значение
        :param dct: передаваемый словарь
        :param tp: какое значение вернуть в случае ошибки
        :return: значение из словаря или значение из блока except
        """
        try:
            return dct[x]
        except KeyError:
            if tp:
                return 'нет данных'
            return 0

    def update_median_table(self, year: int, sq_main: str, sq_fish: str, need_table: str, **kwargs):
        """
        Обновление данных в промежуточной таблице
        Происходит основная трансформация и очистка данных
        :param year: год, за который мы обновляем данные
        :param sq_main: sql скрипт основных данных торговли
        :param sq_fish: sql скрипт данных по рыбе-8
        :param need_table: название промежуточной таблицы
        :param kwargs: дополнительные параметры
        """
        # Зачищаем данные, если такие уже есть в таблице, чтобы избежать задвоения
        self.click_house_client.command(f"DELETE FROM {need_table} WHERE period = {year}")

        # Получаем необходимые словари из переданного контекста
        dct_ref_flow = kwargs['ti'].xcom_pull(task_ids='ref_group_app.data_dict_ref_flow')
        dct_ref_mirror = kwargs['ti'].xcom_pull(task_ids='ref_group_app.data_dict_ref_mirror')
        dct_ref_source = kwargs['ti'].xcom_pull(task_ids='ref_group_app.data_dict_ref_source')

        # Получаем промежуточный датафрейм для каждого месяца
        chunk_df_main = pd.read_sql(sq_main.format(year=year),
                                    con=self.sqlalchemy_con)
        chunk_df_fish = pd.read_sql(sq_fish.format(year=year),
                                    con=self.sqlalchemy_con)
        chunk_df_main = pd.concat((chunk_df_main, chunk_df_fish))

        # Меняем на нужные названия
        chunk_df_main.rename(columns={'trade_flow': 'trade_flow_code', 'year': 'period'}, inplace=True)

        # Преобразуем данные и меняем их на id из словарей
        chunk_df_main['trade_flow_code'] = chunk_df_main['trade_flow_code'].apply(
            lambda x: self.return_value(x, dct_ref_flow))
        chunk_df_main['source'] = chunk_df_main['source'].apply(
            lambda x: self.return_value(x, dct_ref_source))
        chunk_df_main['mirror_columns'] = chunk_df_main['mirror_columns'].apply(
            lambda x: self.return_value(x, dct_ref_mirror))


        # Добавляем новый столбец и преобразуем данные к необходимому типу
        chunk_df_main['update_mart'] = datetime.now().strftime('%Y-%m-%d')
        chunk_df_main = chunk_df_main.astype({'update_mart': 'datetime64[ns]',
                                              'update_date': 'datetime64[ns]'})

        # Загрузка данных в промежуточную таблицу
        self.click_house_client.insert_df(table=need_table, df=chunk_df_main)
        logging.info(f"Загружен год {year} в количестве = {chunk_df_main.shape[0]}")

    def insert_datamart(self, table_source: str, table_update: str):
        """
        Функция обновления основной таблицы витрины месячных данных
        :param table_source: название таблицы источника (наша промежуточная таблица)
        :param table_update: название основной таблицы (витрины)
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_update}')
        self.click_house_client.command(f"""INSERT INTO {table_update}
                            SELECT * FROM {table_source}""")