import clickhouse_connect
import pandas as pd
import logging
import psycopg2


class Update_region_datamart:
    """
    Класс основной логики обновления витрины региональных данных
    """
    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict):
        """
        :param dict_click_cred: креды подключения к ClickHouse

        :param dict_postgre_cred: креды подключения к Postgre
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

    def update_ref_region(self, sql_code_list: list[str], need_table_list: list[str]):
        """
        Обновление справочников регионального дашборда
        :param sql_code_list: список с sql скриптами
        :param need_table_list: список с названиями таблиц в ClickHouse
        """
        for sql, table in zip(sql_code_list, need_table_list):
            df_ref = pd.read_sql(sql, con=self.sqlalchemy_con)
            self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table}')
            self.click_house_client.insert_df(table=table, df=df_ref)

    def update_region_datamart(self, sql_datamart: str, table_name_mart: str, sql_code_list: list[str],
                               need_table_list: list[str], **kwargs):
        """
        :param sql_datamart: sql скрипт региональной витрины данных
        :param table_name_mart: имя обновляемой таблицы в ClickHouse
        :param sql_code_list: список с sql скриптами
        :param need_table_list: список с названиями таблиц в ClickHouse
        """
        try:
            if kwargs["dag_run"].conf['ref']:
                self.update_ref_region(sql_code_list, need_table_list)
        except KeyError:
            logging.info('Справочники не будут обновлены')

        df = pd.read_sql(sql_datamart.format(eac=kwargs["dag_run"].conf['eac'],
                                             nnnn=kwargs["dag_run"].conf['nnnn'],
                                             mptrg=kwargs["dag_run"].conf['mptrg']),
                         con=self.sqlalchemy_con)
        df['date_update'] = df.period.max()
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_name_mart}')
        self.click_house_client.insert_df(table=table_name_mart, df=df)

    def update_table_by_insert(self, script: str, table_name: str):
        """
        :param script: sql скрипт собирающий таблицу на базе ClicHouse
        :param table_name: имя обновляемой таблицы
        :return:
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_name}')
        self.click_house_client.command(f"""INSERT INTO {table_name}
                                            {script}""")

    def update_median_table(self, year: int, sq_main: str, need_table: str, **kwargs):
        """
        Обновление данных в промежуточной таблице
        Происходит основная трансформация и очистка данных
        :param year: год, за который мы обновляем данные
        :param sq_main: sql скрипт основных данных торговли
        :param need_table: название промежуточной таблицы
        :param kwargs: дополнительные параметры
        """
        # Зачищаем данные, если такие уже есть в таблице, чтобы избежать задвоения
        # self.click_house_client.command(f"DELETE FROM {need_table} WHERE year LIKE('%{year}')")

        df = pd.read_sql(sq_main.format(year=year), con=self.sqlalchemy_con)
        self.click_house_client.insert_df(table=need_table, df=df)

    def update_script_not_params(self, sq_main: str, need_table: str):

        df = pd.read_sql(sq_main, con=self.sqlalchemy_con)
        self.click_house_client.insert_df(table=need_table, df=df)

    def update_data_comparable(self, sq_main: str, need_table: str, **kwargs):

        df = pd.read_sql(sq_main.format(eac=kwargs["dag_run"].conf['eac'],
                                        nnnn=kwargs["dag_run"].conf['nnnn'],
                                        mptrg=kwargs["dag_run"].conf['mptrg']),
                         con=self.sqlalchemy_con)
        self.click_house_client.insert_df(table=need_table, df=df)
    def insert_datamart(self, table_source: str, table_update: str):
        """
        Функция обновления основной таблицы витрины месячных данных
        :param table_source: название таблицы источника (наша промежуточная таблица)
        :param table_update: название основной таблицы (витрины)
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_update}')
        self.click_house_client.command(f"""INSERT INTO {table_update}
                        SELECT * FROM {table_source}""")
