import logging

import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine
import telebot
from requests.exceptions import Timeout



class Load_usda_reference_data:
    """
    Класс основной логики парсинга маркетингового года USDA
    """
    def __init__(self, dict_postgre_cred: dict, get_availability_by_commodity: str, get_commodity_code_url: str, token: str):
        """
        :param dict_postgre_cred: словарь с подключением к PostgreSql

        :param get_availability_by_commodity: api ссылка на нужный ресурс

        :param get_commodity_code_url: api ссылка на писок кодов

        :param token: токен бота
        """
        # Подключение к базе PostgreSql
        self.psycopg2_connect = psycopg2.connect(user=dict_postgre_cred['user'],
                                               password=dict_postgre_cred['password'],
                                               host=dict_postgre_cred['host'],
                                               port=dict_postgre_cred['port'],
                                               database=dict_postgre_cred['database'])
        self.sqlalchemy_con = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'
                                            .format(dict_postgre_cred['user'],
                                                    dict_postgre_cred['password'],
                                                    dict_postgre_cred['host'],
                                                    dict_postgre_cred['port'],
                                                    dict_postgre_cred['database']))

        self.get_availability_by_commodity = get_availability_by_commodity
        self.get_commodity_code_url = get_commodity_code_url
        self.path_file_dir = '/need_meta_file_airflow/'
        self.chat_id_admin = 11
        self.bot = telebot.TeleBot(token)

    def get_ref_dict(self, sql_query: str, col_key: str, col_value: str, **kwargs) -> dict:
        """
        :param sql_query: sql запрос
        :param col_key: столбец, который будет ключом в словаре
        :param col_value: столбец, который будет значение в словаре
        :param kwargs: дополнительные параметры
        :return: словарь созданный из справочника
        """
        df = pd.read_sql(sql_query, con=self.psycopg2_connect)
        return {k: v for k, v in zip(df[col_key].tolist(), df[col_value].tolist())}


    def get_commodity_code_usda(self):
        """
        Получаем перечень кодов из USDA и записываем их в файл, для дальнейшего формирования тасок
        """
        json_code = requests.get(self.get_commodity_code_url).json()
        list_code = ' '.join([i['commodityCode'] for i in json_code])
        with open(self.path_file_dir + 'list_code_usda2.txt', 'w') as fl:
            fl.write(list_code)

    def get_list_code_in_task_group(self) -> list[str]:
        """
        Читаем содержимое файла и возвращаем его

        :return: список записанных кодов
        """
        with open(self.path_file_dir + 'list_code_usda2.txt', 'r') as fl:
            list_code = fl.readline().split()
        return list_code

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
            return 9999999

    def load_data_in_db(self, code:str, **kwargs):
        """
        Обновление данных по переданному коду,
        И лаертинг, в случае если какая-то страна отсутствует в справочнике

        :param code: переданный commodity_code

        :param kwargs: набор параметров
        """
        # Получаем сформированные справочники
        dict_country_name = kwargs['ti'].xcom_pull(task_ids='get_ref_data.data_dict_ref_country_add')
        flag_response = True
        # Получаем данные по полученному коду
        while flag_response:
            try:
                response_data = requests.get(self.get_availability_by_commodity.format(code=code), timeout=10).json()
                flag_response = False
            except Timeout:
                logging.exception('Запрос не удался, пробуем еще')
        df_usda_ref = pd.DataFrame(response_data)

        # Отбираем данные от 2023 года
        df_usda_ref = df_usda_ref[df_usda_ref['maxYear'] >= 2023]
        df_usda_ref.rename(columns={'commodity': 'commodity_code'}, inplace=True)
        df_usda_ref.rename(columns=lambda x: x.lower(), inplace=True)
        df_usda_ref['commodity_code'] = code

        # Получаем коды страны и в случае ее отсутствия алертим администратору об этом
        df_usda_ref['country_code'] = df_usda_ref['country'].apply(lambda x: self.validate_key_dict(dict_country_name, x.strip()))
        if df_usda_ref[df_usda_ref['country_code'] == 9999999].shape[0] != 0:
            list_miss_country = ','.join([i for i in df_usda_ref[df_usda_ref['country_code'] == 9999999].country.unique()])
            self.bot.send_message(self.chat_id_admin, "Страны отсутствующие в USDA по сезонам: " + list_miss_country)

        # Записываем данные в базу
        df_usda_ref = df_usda_ref[df_usda_ref['country_code'] != 9999999]
        df_usda_ref.drop(columns=['country'], inplace=True)
        df_usda_ref.to_sql('reference_data', con=self.sqlalchemy_con, schema='usda', if_exists='append', index=False)

