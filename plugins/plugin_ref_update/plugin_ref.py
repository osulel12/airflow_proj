import pandas as pd
import psycopg2
import clickhouse_connect


class Update_ref_table:

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

    @staticmethod
    def del_extra_code(value_columns: str, name_column: str) -> str:
        """
        Удаляем лишние символы в названиях групп
        :param value_columns: значение из столбца
        :param name_column: название столбца
        :return: измененное значение
        """
        if '4' not in name_column:
            return value_columns if value_columns is None or not set('()') <= set(value_columns) \
                else value_columns[:value_columns.rfind('(') - 1]
        elif '4' in name_column and 'FAO' in value_columns:
            code_replace_fao = value_columns[:value_columns.rfind(' / FAO')] + ')'
            return code_replace_fao
        else:
            return value_columns

    def run_scripts(self, sql_script: str, name_clickhouse_table):
        """
        Обновление переданных таблиц справочников
        :param sql_script: скрипт обновляемой таблицы
        :param name_clickhouse_table: нужная таблица в базе данных ClickHouse
        """
        df_pg = pd.read_sql(sql_script, con=self.psycopg_connect)
        if name_clickhouse_table == 'ref_fao_product':
            for i in [col for col in df_pg.columns if set('group_fao') <= set(col)]:
                df_pg[i] = df_pg[i].apply(lambda x: self.del_extra_code(x, i))
        df_pg.fillna('', inplace=True)
        if name_clickhouse_table == 'ref_tnved_postgres':
            df_pg['group5_lv_3'] = df_pg['group5_lv_3'].apply(lambda x: x[:x.rfind('(') - 1])
        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {name_clickhouse_table}")
        self.click_house_client.insert_df(table=name_clickhouse_table, df=df_pg)
