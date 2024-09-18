import os
import shutil
import logging
import requests
from dotenv import load_dotenv
import zipfile
from pathlib import Path
import json
import pandas as pd
from sqlalchemy import create_engine

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


def mkdir_or_del_temp_load_file(flag: str = 'mkdir', **kwargs):
    """
    Функция создает или удаляет временную директорию
    :param flag: упарвлением режимом "удалить" или "создать"
    :param kwargs: набор дополнительных параметров контекста
    """

    if flag == 'mkdir':
        # Создаем временную директорию для хранения файлов
        if not os.path.isdir('tmp_file_load'):
            os.mkdir('tmp_file_load')
            logging.info(f'Список директорий {os.listdir()}')
        kwargs['ti'].xcom_push(value='/tmp_file_load/', key='dir_location')
    else:
        if os.path.isdir('tmp_file_load'):
            shutil.rmtree('tmp_file_load')
            logging.info(os.listdir())


def func_upload_data(**kwargs):
    """
    Функция скачивания данных из json
    """
    ph = kwargs['ti'].xcom_pull(task_ids='create_tmp_dir', key='dir_location')
    actual_json = requests.get(os.getenv('SRC_FAO_JSON')).json()
    need_data_name = [i[0] for i in kwargs['ti'].xcom_pull(task_ids='get_name_file_and_table')]
    dct_name_columns_in_table = {i[1]: i[2] for i in kwargs['ti'].xcom_pull(task_ids='get_name_file_and_table')}
    need_src = {i['DatasetName']: i['FileLocation'] for i in actual_json['Datasets']['Dataset'] if
                i['DatasetName'] in need_data_name}
    logging.info(need_src)

    # Скачивание архивов с файлами и их распаковка
    for name, src in need_src.items():
        s = requests.get(src)
        with open(ph + "s.zip", "wb") as code:
            code.write(s.content)
        with zipfile.ZipFile(ph + "s.zip", 'r') as zip_file:
            zip_file.extractall(ph)

    # Получаем путь к нужным файлам и название таблиц
    dct_to_json = {}
    read_files = [str(i) for i in Path(ph).glob('**/*(Normalized).csv')]
    list_table_in_db = [
        'foodbalancesheets' if Path(i).stem.split('_E_All')[0].lower() == 'commoditybalances_(non-food)_(2010-)' else
        Path(i).stem.split('_E_All')[0].lower() for i in read_files]
    for k, v in zip(list_table_in_db, read_files):
        if k in dct_to_json:
            dct_to_json[k].append((v, dct_name_columns_in_table[k]))
        else:
            dct_to_json[k] = [(v, dct_name_columns_in_table[k])]

    # Сохраняем в файл, для дальнейшей возможности генерации task group
    with open('/need_meta_file_airflow/name_table_and_file_path.json', 'w', encoding='utf-8') as fl:
        json.dump(dct_to_json, fl, indent=4, ensure_ascii=False)


def loading_temporary_table(table: str, file_ph: list, **kwargs):
    """
    Обновление переходной таблицы
    :param table: название таблицы
    :param file_ph: путь к файлу, а так же название колонок у переданной таблицы в базе данных
    :param kwargs: дополнительные параметры
    """
    count_row = 0
    conn = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'
                         .format(kwargs['templates_dict']['user'], kwargs['templates_dict']['password'],
                                 kwargs['templates_dict']['host'], kwargs['templates_dict']['port'],
                                 kwargs['templates_dict']['database']))
    # Обновление таблицы
    for path in file_ph:
        df_temp = pd.read_csv(path[0], encoding='cp1250', chunksize=100000)
        for iter_df in df_temp:
            # logging.info(iter_df.head())
            iter_df.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)
            # logging.info(path[1].split())
            iter_df = iter_df[path[1].split()]
            # logging.info(iter_df.head())
            count_row += iter_df.shape[0]
            iter_df.to_sql(table, con=conn, schema=os.getenv('SCHEMA_ETL'), if_exists='append', index=False)


# Скрипт получения информации о каждой из таблиц
sql_get_nameFile_and_table = f"""SELECT dataset_name_in_json, name_table_in_db, columns_table 
                                FROM {os.getenv('SCHEMA_ETL')}.{os.getenv('NAME_TABLE_UPDATE_FAO')}"""

