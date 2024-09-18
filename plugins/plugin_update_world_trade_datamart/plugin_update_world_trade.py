import clickhouse_connect
import pandas as pd
from datetime import datetime
import logging
import psycopg2




class Update_world_trade_datamart:
    """
    Класс основной логики обновления витрины годовых данных мировой торговли
    """

    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict):
        """
        :param dict_postgre_cred: словарь с параметрами подключения к базе данных Postgre

        :param dict_click_cred: словарь с параметрами подключения к базе данных Clickhouse
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

    def truncate_median_table(self, table_name: str, **kwargs):
        """
        Очистка промежуточной таблицы обновления месячных данных
        :param table_name: имя очищаемой таблицы
        :param kwargs: дополнительные параметры
        """
        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")

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
        self.click_house_client.command(f"DELETE FROM {need_table} WHERE year = {year}")

        # Получаем промежуточный датафрейм для каждого месяца
        df_main = pd.read_sql(sq_main.format(year=year),
                              con=self.sqlalchemy_con, chunksize=200000)
        df_fish = pd.read_sql(sq_fish.format(year=year),
                              con=self.sqlalchemy_con)
        for i, chunk in enumerate(df_main):
            if i == 0:
                chunk = pd.concat((chunk, df_fish))

            chunk['group_prod2'] = chunk['group_prod2'].fillna('Прочая продукция')
            chunk['switch_mpt'] = chunk.commodity_code.apply(
                lambda x: 'продукция АПК c кодами ТН ВЭД 01–24' if int(
                    str(x)[:2]) < 25 else 'продукция АПК с кодами ТН ВЭД  выше 24-го')
            chunk_df_for_load = chunk.groupby(
                ['year', 'trade_flow', 'reporter_code', 'reporter', 'partner_code', 'partner', 'group_prod1',
                 'group_prod2', 'source', 'mirror_columns', 'update_date', 'switch_mpt'], as_index=False) \
                .agg({'trade_value': 'sum', 'netweight': 'sum'})
            chunk_df_for_load['update_mart'] = datetime.now().now().strftime('%Y-%m-%d')
            chunk_df_for_load['group_prod2'] = chunk_df_for_load.group_prod2.apply(
                lambda x: x if x is None or not set('()') <= set(x) else x[:x.rfind('(') - 1])
            chunk_df_for_load = chunk_df_for_load.astype({'update_mart': 'datetime64[ns]',
                                                          'update_date': 'datetime64[ns]'})

            # Загрузка данных в промежуточную таблицу
            self.click_house_client.insert_df(table=need_table, df=chunk_df_for_load)
            logging.info(f"Загружен год {year} итерации {i} в количестве = {chunk_df_for_load.shape[0]}")

    def insert_datamart(self, table_source: str, table_update: str):
        """
        Функция обновления основной таблицы витрины месячных данных
        :param table_source: название таблицы источника (наша промежуточная таблица)
        :param table_update: название основной таблицы (витрины)
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_update}')
        self.click_house_client.command(f"""INSERT INTO {table_update}
                            SELECT * FROM {table_source}""")
