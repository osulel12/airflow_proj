import clickhouse_connect
import pandas as pd
from datetime import datetime
import logging
import psycopg2


class Update_balance_datamart:
    """
    Класс основной логики обновления витрины данных Балансов
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

    @staticmethod
    def validate_key_dict(dict_ref: dict, value: str) -> int:
        """
        Проверка наличия ключа в переданном словаре

        :param dict_ref: словарь сформированный из справочника

        :param value: значение, которое проверяем

        :return: id значения в справочнике
        """
        try:
            return dict_ref[value]
        except KeyError:
            return 777

    def update_median_table(self, year: int, sq_fao: str, sq_usda: str, need_table: str, **kwargs):
        """
        Обновление данных в промежуточной таблице
        Происходит основная трансформация и очистка данных

        :param year: год, за который мы обновляем данные

        :param sq_fao: sql скрипт данных баланса по FAO

        :param sq_usda: sql скрипт данных баланса по USDA

        :param need_table: название промежуточной таблицы

        :param kwargs: дополнительные параметры
        """
        # Зачищаем данные, если такие уже есть в таблице, чтобы избежать задвоения
        self.click_house_client.command(f"DELETE FROM {need_table} WHERE period = {year}")

        # Получаем необходимые словари из переданного контекста
        dct_ref_flow = kwargs['ti'].xcom_pull(task_ids='ref_group_balance.data_dict_ref_flow')
        dct_ref_flow_usda = kwargs['ti'].xcom_pull(task_ids='ref_group_balance.data_dict_ref_flow_usda')
        dct_ref_source = kwargs['ti'].xcom_pull(task_ids='ref_group_balance.data_dict_ref_source')
        dct_ref_fao_product = kwargs['ti'].xcom_pull(task_ids='ref_group_balance.data_dict_ref_fao_product')
        dct_ref_mirror = kwargs['ti'].xcom_pull(task_ids='ref_group_balance.data_dict_ref_mirror')

        # Получаем промежуточный датафрейм
        chunk_df_fao = pd.read_sql(sq_fao.format(year=year),
                                   con=self.sqlalchemy_con)
        chunk_df_usda = pd.read_sql(sq_usda.format(year=year),
                                    con=self.sqlalchemy_con)
        chunk_df_fao.dropna(subset=['group_fao_lv4'], inplace=True)
        chunk_df_usda.dropna(subset=['group_fao_lv4'], inplace=True)
        for i in [col for col in chunk_df_fao.columns if set('group_fao') <= set(col)]:
            chunk_df_fao[i] = chunk_df_fao[i].apply(lambda x: self.del_extra_code(x, i))
        for i in [col for col in chunk_df_usda.columns if set('group_fao') <= set(col)]:
            chunk_df_usda[i] = chunk_df_usda[i].apply(lambda x: self.del_extra_code(x, i))

        # Для FAO
        chunk_df_fao = chunk_df_fao[
            ~chunk_df_fao.apply(lambda x: x['type_flow'] == 'Production' and x['unit'] != 't', axis=1)]
        chunk_df_fao['valid_column'] = chunk_df_fao.apply(lambda x: str(x['reporter_code']) + x['group_fao_lv4'],
                                                          axis=1)
        l_exp = chunk_df_fao.query('type_flow == "Export"').valid_column.unique()
        l_imp = chunk_df_fao.query('type_flow == "Import"').valid_column.unique()
        l_prod = chunk_df_fao.query('type_flow == "Production"').valid_column.unique()
        n_l = set(l_prod) & set(l_imp) & set(l_exp)
        chunk_df_fao['bool'] = chunk_df_fao.valid_column.apply(lambda x: x in n_l)
        chunk_df_fao = chunk_df_fao[chunk_df_fao['bool']]
        chunk_df_fao.drop(columns=['valid_column', 'bool'], inplace=True)
        d_exp = chunk_df_fao.query('type_flow == "Export"')[['year', 'reporter_code', 'group_fao_lv4', 'value']].rename(
            columns={'value': 'value_exp'})
        d_imp = chunk_df_fao.query('type_flow == "Import"')[['year', 'reporter_code', 'group_fao_lv4', 'value']]
        d_prod = chunk_df_fao.query('type_flow == "Production"')[['year', 'reporter_code', 'group_fao_lv4', 'value']]
        df_cons = d_prod.merge(d_imp, on=['year', 'reporter_code', 'group_fao_lv4'], how='inner',
                               suffixes=('_prod', '_imp')) \
            .merge(d_exp, on=['year', 'reporter_code', 'group_fao_lv4'], how='inner')
        df_cons['value'] = df_cons.value_prod + df_cons.value_imp - df_cons.value_exp
        df_cons = df_cons[['year', 'reporter_code', 'group_fao_lv4', 'value']]
        df_cons = df_cons.assign(type_flow='consumption', source='Балансы', mirror_columns='прямые данные',
                                 unit='t')
        chunk_df_fao = pd.concat((chunk_df_fao, df_cons))

        chunk_df_fao['type_flow'] = chunk_df_fao['type_flow'].apply(lambda x: self.validate_key_dict(dct_ref_flow, x))
        chunk_df_usda['type_flow'] = chunk_df_usda['type_flow'].apply(lambda x: self.validate_key_dict(dct_ref_flow_usda, x))
        df_mart = pd.concat((chunk_df_fao, chunk_df_usda))
        df_mart = df_mart[df_mart['type_flow'] != 777]

        df_mart.rename(columns={'year': 'period'}, inplace=True)
        df_mart['group_fao_lv4'] = df_mart['group_fao_lv4'].apply(lambda x: dct_ref_fao_product[x])
        df_mart['source'] = df_mart['source'].apply(lambda x: dct_ref_source[x])
        df_mart['mirror_columns'] = df_mart['mirror_columns'].apply(lambda x: dct_ref_mirror[x])
        df_mart['update_mart'] = datetime.now().now().strftime('%Y-%m-%d')
        df_mart['seasonality'] = df_mart.period
        df_mart = df_mart.astype({'update_mart': 'datetime64[ns]'})

        # Загрузка данных в промежуточную таблицу
        self.click_house_client.insert_df(table=need_table, df=df_mart)
        logging.info(f"Загружен год {year} в количестве = {df_mart.shape[0]}")

    def insert_datamart(self, table_source: str, table_update: str):
        """
        Функция обновления основной таблицы витрины месячных данных
        :param table_source: название таблицы источника (наша промежуточная таблица)
        :param table_update: название основной таблицы (витрины)
        """
        self.click_house_client.command(f'TRUNCATE TABLE IF EXISTS {table_update}')
        self.click_house_client.command(f"""INSERT INTO {table_update}
                        SELECT * FROM {table_source}""")
