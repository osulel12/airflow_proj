import pandas as pd
import logging
from sqlalchemy import create_engine
import requests
from datetime import datetime, timedelta
from io import StringIO


class Load_data_from_moex:
    """
    Сбор данных биржи MOEX
    """
    def __init__(self, dict_postgre_cred: dict, meta_src: str, data_src: str):
        # Подключение к базе PostgreSql
        self.sqlalchemy_con = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'
                                            .format(dict_postgre_cred['user'],
                                                    dict_postgre_cred['password'],
                                                    dict_postgre_cred['host'],
                                                    dict_postgre_cred['port'],
                                                    dict_postgre_cred['database']))
        # Ссылка на метаинформацию о sec_id
        self.meta_src = meta_src
        # Ссылка на данные
        self.data_src = data_src

    def get_boards_data(self, sec_id: str) -> tuple:
        """
        Собираем мета информацию об индексе и получаем мнимальную дату и максимальную дату данных
        :param sec_id: наименование индекса
        :return: картеж с минимальной датой и максимальной датой данных по индексу
        """
        response = requests.get(self.meta_src.format(sec_id=sec_id)).json()
        from_date = response['boards']['data'][0][response['boards']['columns'].index('listed_from')]
        till_date = response['boards']['data'][0][response['boards']['columns'].index('listed_till')]
        return from_date, till_date

    def cleare_all_data(self, **kwargs):
        """
        Очистка таблицы при ручном триггере dag
        :param kwargs: набор параметров dar_run
        :return:
        """
        if kwargs['ti'].xcom_pull(task_ids='task_create_xcom', key='trigger_cleare') == 'cleare_table':
            with self.sqlalchemy_con.connect() as con:
                con.execution_options(isolation_level="AUTOCOMMIT")
                with con.begin():
                    con.execute(f'TRUNCATE TABLE es.moex_index_data')
                    con.execute('VACUUM (FULL, ANALYZE) es.moex_index_data')
        else:
            logging.info('Запуск по расписанию')

    @staticmethod
    def create_xcom_for_task(**kwargs):
        """
        Формируем xcom в заависимости от типа запуска dag
        :param kwargs: набор параметров dar_run
        :return:
        """
        try:
            if kwargs['dag_run'].conf['all_cleare'] == 'all cleare':
                kwargs['ti'].xcom_push(value='cleare_table', key='trigger_cleare')
            else:
                kwargs['ti'].xcom_push(value='not_cleare_table', key='trigger_cleare')
        except KeyError:
            kwargs['ti'].xcom_push(value='not_cleare_table', key='trigger_cleare')
            logging.info('Запуск по расписанию')

    def get_max_period_for_sec_id(self, sec_id: str) -> str:
        """
        Максимальный период индекса в БД
        :param sec_id: Наименование индекса
        :return: дата с типом данных строка
        """
        try:
            with self.sqlalchemy_con.begin() as cur:
                return cur.execute(f"SELECT MAX(period) FROM es.moex_index_data WHERE secid = '{sec_id}'").fetchone()[0].strftime('%Y-%m-%d')
        except AttributeError:
            return datetime.now().strftime('%Y-%m-%d')

    def get_data_index_stock(self, sec_id: str, **kwargs):
        """
        Загрузка данных по переданному индексу
        :param sec_id: Наименование индекса
        :param kwargs: набор параметров dar_run
        :return:
        """
        try:
            if kwargs['dag_run'].conf['all_cleare']:
                from_date, till_date = self.get_boards_data(sec_id)
        except KeyError:
            from_date, till_date = ((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                                    (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        max_period_sec_id = self.get_max_period_for_sec_id(sec_id)
        logging.info(f'С {from_date} по {till_date}')
        for step in range(0, 1000000, 100):
            src = requests.get(self.data_src.format(sec_id=sec_id, from_date=from_date,
                                                    till_date=till_date, step=step))
            df = pd.read_xml(StringIO(src.text), xpath=".//row")
            if df.shape[0] == 1:
                break
            df.query("TRADEDATE != @max_period_sec_id", inplace=True)
            if df.shape[0] == 1:
                break
            df = df[['SECID', 'TRADEDATE', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME']]
            df.rename(columns=lambda x: 'period' if x == 'TRADEDATE' else x.lower(), inplace=True)
            df.dropna(subset=['secid'], inplace=True)
            df.to_sql('moex_index_data', schema='prices', if_exists='append', index=False, con=self.sqlalchemy_con)
