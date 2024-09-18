import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import telebot
from telebot.types import InputMediaDocument
import pathlib
import os
import re
from contextlib import ExitStack
from datetime import datetime


class Update_new_certificates_date:
    """
    Класс основной логики работы с датами страновых справок Управления аналитики внешних рынков
    """

    def __init__(self, dict_postgre_cred: dict):
        """
        :param dict_postgre_cred: словарь с подключением к PostgreSql
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

        self.list_patern_forma2 = ['**/*форма2,*.docx', '**/*форма 2,*.docx', '**/*форма_2,*.docx']
        self.list_patern_forma2a = ['**/*форма 2а,*.docx', '**/*форма2а,*.docx', '**/*форма_2а,*.docx',
                                    '**/*форма 2a,*.docx', '**/*форма2a,*.docx', '**/*форма_2a,*.docx']
        self.list_patern_forma1_docx = ['**/*форма1,*.docx', '**/*форма 1,*.docx', '**/*форма_1,*.docx']
        self.list_patern_forma1_xlsx = ['**/*форма 1,*.xlsx', '**/*форма1,*.xlsx', '**/*форма_1,*.xlsx']
        self.list_patern_reference = ['**/*Справка*.docx', '**/*Справка*.doc']
        self.list_patern_potential_xlsx = ['**/*потенц*.xlsx', '**/*Потенц*.xlsx', '**/*Потенц*.xlsb', '**/*потенц*.xlsb']
        self.list_patern_potential_doc = ['**/*потенц*.doc', '**/*Потенц*.doc', '**/*потенц*.docx', '**/*Потенц*.docx']
        self.path_save_file = '/need_meta_file_airflow/'

    def get_key_value_in_db(self) -> list[str]:
        """
        :return: возврщает список ключей из таблицы current_information_references
        """
        with self.psycopg2_connect.cursor() as cur:
            cur.execute("""SELECT key FROM tl.current_information_references""")
            return [i[0] for i in cur.fetchall()]

    def search_form2(self, path_to_files: pathlib.Path) -> list:
        """
        :path_to_files: путь к папке с файлами

        :return: список из пути самого акутального файла и его дуту создания
        """
        temp_path_f2 = ''
        temp_date_f2 = ''
        count_f2 = 0
        for patern in self.list_patern_forma2:
            if len(list(path_to_files.glob(patern))) > 0:
                for i in path_to_files.glob(patern):
                    if count_f2 == 0:
                        temp_path_f2 = i
                        temp_date_f2 = os.path.getctime(i)
                    elif temp_date_f2 < os.path.getctime(i):
                        temp_date_f2 = os.path.getctime(i)
                        temp_path_f2 = i
                    count_f2 += 1
        return [temp_path_f2, temp_date_f2]

    def search_form1(self, path_to_files: pathlib.Path) -> list[tuple]:
        """
        :path_to_files: путь к папке с файлами

        :return: список из картежей, где первый элемент это пути самого акутального файла,
                 а второй дата создания
        """
        temp_path_f1 = ''
        temp_date_f1 = ''
        count_f1 = 0
        temp_path_exl = ''
        temp_date_exl = ''
        count_exl = 0
        for docx in self.list_patern_forma1_docx:
            if len(list(path_to_files.glob(docx))) > 0:
                for d in path_to_files.glob(docx):
                    if count_f1 == 0:
                        temp_path_f1 = d
                        temp_date_f1 = os.path.getctime(d)
                    elif temp_date_f1 < os.path.getctime(d):
                        temp_date_f1 = os.path.getctime(d)
                        temp_path_f1 = d
                    count_f1 += 1

        for xlsx in self.list_patern_forma1_xlsx:
            if len(list(path_to_files.glob(xlsx))) > 0:
                for x in path_to_files.glob(xlsx):
                    if count_exl == 0:
                        temp_path_exl = x
                        temp_date_exl = os.path.getctime(x)
                    elif temp_date_exl < os.path.getctime(x):
                        temp_date_exl = os.path.getctime(x)
                        temp_path_exl = x
                    count_exl += 1
        return [(temp_path_f1, temp_date_f1), (temp_path_exl, temp_date_exl)]

    def get_actual_form2(self, path_to_files: pathlib.Path, folder: str, country_or_group: str):
        """
        Для управления Внешней торговли
        Функция запиывает в БД дату актуальной справки по форме2 в переданной папке

        :param path_to_files: путь к папкам, где будем искать справки

        :param folder: название папки (страна или группа стран) в которой смотрим дату справки

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """

        need_path_file = self.search_form2(path_to_files)[0]
        try:
            date_update = re.search(r'\d\d\d\d[._]\d\d[._]\d\d', str(need_path_file))[0]
            period_update = str(need_path_file).split(',')[-1].split(')')[0]
        except TypeError:
            date_update = None
            period_update = None

        with self.psycopg2_connect.cursor() as cur:
            cur.execute("""INSERT INTO tl.latest_version_files 
                                 VALUES(%s, %s, %s, %s, %s)""",
                        (folder, 'Форма 2', date_update, period_update, country_or_group))
        self.psycopg2_connect.commit()

    def get_actual_form2_per_day(self, path_to_files: pathlib.Path, folder: str, key_in_db: list,
                                 country_or_group: str):
        """
        Для управления Внешней торговли
        Функция обновляет в БД дату справки по форме2 относительно крайнего запуска task

        :param path_to_files: путь к папкам, где будем искать справки

        :param folder: название папки (страна или группа стран) в которой смотрим дату справки

        :param key_in_db: ключи из базы данных

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """
        need_path_file, need_date_create_file = self.search_form2(path_to_files)
        try:
            references_name = need_path_file.name
            now_key = folder + ' Форма2'

            with self.psycopg2_connect.cursor() as cur:
                if now_key not in key_in_db:
                    cur.execute("""INSERT INTO tl.current_information_references
                                         VALUES(%s, %s, %s, %s, %s)""",
                                (references_name, now_key, need_date_create_file, need_date_create_file,
                                 country_or_group))
                else:
                    cur.execute("""UPDATE tl.current_information_references
                                   SET date_update_yesterday = date_update_now,
                                       date_update_now = %s,
                                       references_name = %s
                                   WHERE key = %s""", (need_date_create_file, references_name, now_key))
            self.psycopg2_connect.commit()
        except AttributeError:
            logging.exception(f'{need_path_file}: {type(need_path_file)}')

    def get_actual_form1(self, path_to_files: pathlib.Path, folder: str, country_or_group: str):
        """
        Для управления Внешней торговли
        Функция запиывает в БД дату актуальной справки по форме1 в переданной папке

        :param path_to_files: путь к папкам, где будем искать справки

        :param folder: название папки (страна или группа стран) в которой смотрим дату справки

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """
        doc_file, xlsx_file = self.search_form1(path_to_files)
        try:
            date_update_doc = re.search(r'\d\d\d\d[._]\d\d[._]\d\d', str(doc_file[0]))[0]
            period_update_doc = str(doc_file[0]).split(',')[-1].split(')')[0]
            date_update_xlsx = re.search(r'\d\d\d\d[._]\d\d[._]\d\d', str(xlsx_file[0]))[0]
            period_update_xlsx = str(xlsx_file[0]).split(',')[-1].split(')')[0]
        except TypeError:
            date_update_doc = None
            period_update_doc = None
            date_update_xlsx = None
            period_update_xlsx = None

        with self.psycopg2_connect.cursor() as cur:
            cur.execute("""INSERT INTO tl.latest_version_files 
                                 VALUES(%s, %s, %s, %s, %s),
                                 (%s, %s, %s, %s, %s)""",
                        (folder, 'Форма 1', date_update_doc, period_update_doc, country_or_group,
                         folder, 'Форма 1 xlsx', date_update_xlsx, period_update_xlsx, country_or_group))
        self.psycopg2_connect.commit()

    def get_actual_form1_per_day(self, path_to_files: pathlib.Path, folder: str, key_in_db: list,
                                 country_or_group: str):
        """
        Для управления Внешней торговли
        Функция обновляет в БД дату справки по форме1 относительно крайнего запуска task

        :param path_to_files: путь к папкам, где будем искать справки

        :param folder: название папки (страна или группа стран) в которой смотрим дату справки

        :param key_in_db: ключи из базы данных

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """
        doc_file, xlsx_file = self.search_form1(path_to_files)
        try:
            references_name_doc = doc_file[0].name
            now_key_doc = folder + ' Форма1'
            references_name_xlsx = xlsx_file[0].name
            now_key_xlsx = folder + ' Форма1_xlsx'

            with self.psycopg2_connect.cursor() as cur:
                if now_key_doc not in key_in_db:
                    cur.execute("""INSERT INTO tl.current_information_references
                                         VALUES(%s, %s, %s, %s, %s),
                                               (%s, %s, %s, %s, %s)""",
                                (references_name_doc, now_key_doc, doc_file[1], doc_file[1], country_or_group,
                                 references_name_xlsx, now_key_xlsx, xlsx_file[1], xlsx_file[1], country_or_group))
                else:
                    cur.execute("""UPDATE tl.current_information_references
                                   SET date_update_yesterday = date_update_now,
                                       date_update_now = %s,
                                       references_name = %s
                                   WHERE key = %s""", (doc_file[1], references_name_doc, now_key_doc))
                    cur.execute("""UPDATE tl.current_information_references
                                                       SET date_update_yesterday = date_update_now,
                                                           date_update_now = %s,
                                                           references_name = %s
                                                       WHERE key = %s""", (xlsx_file[1], references_name_xlsx, now_key_xlsx))
            self.psycopg2_connect.commit()
        except AttributeError:
            logging.exception(f'{doc_file[0]}: {type(doc_file[0])}')

    def insert_db_current_creation_date_file(self, path: str, country_or_group: str):
        """
        Функция аккумулирует в себе функционал записи актуальных дат справок по форме 1 и 2
        нужна для создания определенного task

        :param path: путь до нужной директории

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """
        folder_list = [i for i in os.listdir(path) if '_' not in i and '.' not in i]
        logging.info(folder_list)

        # Записываем актуальные даты по каждой из папки в базу данных
        for folder in folder_list:
            path_full = path + folder
            way_pah_values = pathlib.Path(path_full)
            logging.info(way_pah_values)
            self.get_actual_form2(way_pah_values, folder, country_or_group)
            self.get_actual_form1(way_pah_values, folder, country_or_group)

    def get_file_from_user(self, bot_token: str, **kwargs):
        """
        Функция сохраняет данные о крайних датах каждой из справок в нужные файлы и отправляет их до пользователя,
        который тригернул ее через ТГ бота.

        :param bot_token: токен бота, через которого будут отправляться файлы
        """
        bot = telebot.TeleBot(bot_token)

        list_name_file = [self.path_save_file + 'Справки по странам.xlsx',
                          self.path_save_file + 'Справки по группам стран.xlsx']

        df_country = pd.read_sql("""SELECT name_directory, form_factor, date_update, period_update 
                                    FROM tl.latest_version_files
                                    WHERE country_or_group = 'country'""", con=self.psycopg2_connect)

        df_country_group = pd.read_sql("""SELECT name_directory, form_factor, date_update, period_update 
                                            FROM tl.latest_version_files
                                            WHERE country_or_group = 'group'""", con=self.psycopg2_connect)

        # Сохраняем датафреймы в файл с учетом длины полей (для удобочитаемости)
        for df_iter, name_file in zip([df_country, df_country_group], list_name_file):

            with pd.ExcelWriter(name_file) as writer:
                df_iter.to_excel(writer, sheet_name='Справки', index=False, na_rep='NaN')
                for column in df_iter:
                    column_width = max(df_iter[column].astype(str).map(len).max(), len(column))
                    col_idx = df_iter.columns.get_loc(column)
                    writer.sheets['Справки'].set_column(col_idx, col_idx,
                                                        len(column) + 10 if column_width > 200 else column_width)
                writer.sheets['Справки'].set_default_row(30)

        # Отправляем файлы пользователю
        with ExitStack() as stack:
            files_open = [stack.enter_context(open(file, 'rb')) for file in list_name_file]
            list_media_group = [
                InputMediaDocument(file_open, caption='Актуальные даты справок') if i == len(
                    files_open) - 1 else InputMediaDocument(file_open)
                for i, file_open in enumerate(files_open)]

            bot.send_media_group(kwargs["dag_run"].conf['chat_id'], list_media_group)

    def insert_db_checking_the_previous_day_certificates(self, path: str, country_or_group: str, **kwargs):
        """
        Функция аккумулирует в себе функционал записи актуальных дат справок по форме 1 и 2
        относительно крайнего запуска дага

        :param path: путь до нужной директории

        :param country_or_group: флаг, к какому типу принадлежит справка (страновая/группы стран)
        """
        folder_list = [i for i in os.listdir(path) if '_' not in i and '.' not in i]
        list_key_in_db = kwargs['ti'].xcom_pull(task_ids='task_get_key_value')
        logging.info(folder_list)

        # Записываем актуальные даты по каждой из папки в базу данных
        for folder in folder_list:
            path_full = path + folder
            way_pah_values = pathlib.Path(path_full)
            logging.info(way_pah_values)
            self.get_actual_form2_per_day(way_pah_values, folder, list_key_in_db, country_or_group)
            self.get_actual_form1_per_day(way_pah_values, folder, list_key_in_db, country_or_group)

    def get_actual_certificates_file_per_day(self) -> list:
        """
        Записывает изменившиеся даты справок в файл, для последующей отправки пользователю

        :return: путь до файла, если файл не пустой, иначе пустой список
        """
        # """SELECT references_name, to_timestamp(date_update_now)::timestamp as date_create
        #                             FROM tl.current_information_references
        #                             WHERE date_update_now > date_update_yesterday"""


        file_name = self.path_save_file + 'Изменения в справках.xlsx'
        df = pd.read_sql("""SELECT references_name, date_update_now
                            FROM tl.current_information_references 
                            WHERE date_update_now > date_update_yesterday""",
                         con=self.psycopg2_connect)
        if df.shape[0] > 0:
            df['date_update_now'] = df['date_update_now'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
            with pd.ExcelWriter(file_name) as writer:
                df.to_excel(writer, sheet_name='Справки', index=False, na_rep='NaN')
                for column in df:
                    column_width = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    writer.sheets['Справки'].set_column(col_idx, col_idx,
                                                        len(column) + 10 if column_width > 200 else column_width)
                writer.sheets['Справки'].set_default_row(30)
            return [file_name]
        else:
            return []

    def get_variables(self, operation_name: str) -> dict:
        """
        :param operation_name: имя операции из БД

        :return: словарь параметров для переданной операции
        """
        with self.psycopg2_connect.cursor() as cur:
            cur.execute("""SELECT variables_dag 
                           FROM bot.status_operation
                           WHERE operation_name = %s""",
                        (operation_name,))
            return cur.fetchone()[0]

    @staticmethod
    def get_count_need_patern_reference(list_patern: list, path_to_files: pathlib.Path,
                                        date_start: datetime, date_end: datetime) -> int:
        """
        :param list_patern: список патернов для поиска файлов

        :param path_to_files: путь к папке, где будет вестись поиск

        :param date_start: старт интервала поиска (включительно)

        :param date_end: конец интервала поиска (не включительно)

        :return: количество найденных файлов в переданной папке
        """
        count = 0
        for patern in list_patern:
            if len(list(path_to_files.glob(patern))) > 0:
                for i in path_to_files.glob(patern):
                    if date_start <= datetime.fromtimestamp(os.path.getmtime(i)) < date_end:
                        count += 1
        return count


    def create_file_report_count_certificates(self, path_list_reference: list[str], path_potential:str,
                                              path_non_standard: str, **kwargs) -> list[str]:
        """
        :param path_list_reference: список путей для поиска справок по форме1 и форме2

        :param path_potential: путь для поиска справок по потенциалам

        :param path_non_standard: путь для поиска не стандартных справок

        :param kwargs: набор параметров запуска dag_run

        :return: список названий файлов, в которые сохранены отчеты
        """
        list_file_name = [self.path_save_file + 'Отчет количество справок.xlsx',
                          self.path_save_file + 'Потенциалы по папкам.xlsx']

        # Списки папок в зависимости от переданных путей
        folder_list_country = [i for i in os.listdir(path_list_reference[0]) if '_' not in i and '.' not in i]
        folder_list_country_group = [i for i in os.listdir(path_list_reference[1]) if '_' not in i and '.' not in i]
        folder_list_potential = [p for p in os.listdir(path_potential) if 'rar' not in p and 'xlsx' not in p and 'lnk' not in p]
        list_dir_non_standart = [non for non in os.listdir(path_non_standard) if 'rar' not in non and 'xlsx' not in non and 'lnk' not in non]

        # Получаем словарь переменных для текущего dag
        dict_variables = kwargs['ti'].xcom_pull(task_ids='task_get_variables')
        logging.info(dict_variables)
        # Преобразуем их в дату
        date_start = datetime.strptime(dict_variables['date_start'], "%Y.%m.%d")
        date_end = datetime.strptime(dict_variables['date_end'], "%Y.%m.%d")

        # Словари для будущих файлов
        dict_from_certuficates = {'Форма_2': [0], 'Форма_1_doc': [0], 'Форма_1_xlsx': [0],
                                  'Форма_2а': [0], 'Файлы_с_названием_Справка': [0],
                                  'Потенциалы_doc': [0], 'Потенциалы_xlsx': [0],
                                  'Справки_Нестандарт': [0],
                                  'date_start': [dict_variables['date_start']], 'date_end': [dict_variables['date_end']]}
        dict_potential_non_struction = {'date_start': [dict_variables['date_start']], 'date_end': [dict_variables['date_end']]}

        # Записываем значения подсчета в словари
        for list_folder, path in zip((folder_list_country, folder_list_country_group), path_list_reference):
            for folder in list_folder:
                path_full = path + folder
                way_pah_values = pathlib.Path(path_full)
                logging.info(way_pah_values)

                # Для формы 2
                dict_from_certuficates['Форма_2'][0] += self.get_count_need_patern_reference(
                    self.list_patern_forma2,
                    way_pah_values, date_start, date_end)

                # Для формы 1 в формате doc и docx
                dict_from_certuficates['Форма_1_doc'][0] += self.get_count_need_patern_reference(
                    self.list_patern_forma1_docx,
                    way_pah_values, date_start,date_end)

                # Для формы 1 в формате xlsx
                dict_from_certuficates['Форма_1_xlsx'][0] += self.get_count_need_patern_reference(
                    self.list_patern_forma1_xlsx,
                    way_pah_values, date_start, date_end)

                # Для формы 2а
                dict_from_certuficates['Форма_2а'][0] += self.get_count_need_patern_reference(
                    self.list_patern_forma2a,
                    way_pah_values, date_start, date_end)

                # Для файлов у которых есть в названии "Справка"
                dict_from_certuficates['Файлы_с_названием_Справка'][0] += self.get_count_need_patern_reference(
                    self.list_patern_reference,
                    way_pah_values, date_start, date_end)

        # Записываем значения в словарь для потенциалов
        for folder_potential in folder_list_potential:

            # Для подсчета потенциалов по папкам
            if folder_potential not in dict_potential_non_struction:
                dict_potential_non_struction[folder_potential] = [0]

            path_full_potential = path_potential + folder_potential
            way_pah_values_potential = pathlib.Path(path_full_potential)

            # Подсчет для потенциалов в формате doc
            potential_doc = self.get_count_need_patern_reference(
                self.list_patern_potential_doc,
                way_pah_values_potential, date_start,
                date_end)

            # Подсчет для потенциалов в формате xlsx
            potential_xlsx = self.get_count_need_patern_reference(
                self.list_patern_potential_xlsx,
                way_pah_values_potential, date_start,
                date_end)

            dict_from_certuficates['Потенциалы_doc'][0] += potential_doc

            dict_from_certuficates['Потенциалы_xlsx'][0] += potential_xlsx


            dict_potential_non_struction[folder_potential][0] += potential_doc + potential_xlsx

        # Запись и подсчет для потенциалов по папкам
        for folder_non_standart in list_dir_non_standart:
            path_full_non_standard = path_non_standard + folder_non_standart
            if date_start <= datetime.fromtimestamp(os.path.getmtime(path_full_non_standard)) < date_end:
                dict_from_certuficates['Справки_Нестандарт'][0] += 1
        # logging.info(dict_from_certuficates)
        # logging.info(dict_potential_non_struction)

        # Записываем данные в файлы
        df_report = pd.DataFrame(dict_from_certuficates)
        df_potential_non_struction = pd.DataFrame(dict_potential_non_struction)
        for file_name, df in zip(list_file_name, [df_report, df_potential_non_struction]):
            with pd.ExcelWriter(file_name) as writer:
                df.to_excel(writer, sheet_name='Количество справок', index=False, na_rep='NaN')
                for column in df:
                    column_width = max(df[column].astype(str).map(len).max(), len(column))
                    col_idx = df.columns.get_loc(column)
                    writer.sheets['Количество справок'].set_column(col_idx, col_idx,
                                                        len(column) + 10 if column_width > 200 else column_width)
                writer.sheets['Количество справок'].set_default_row(30)
        return list_file_name

    @staticmethod
    def get_file_report_from_user(bot_token: str, task_ids: str, **kwargs):
        """
        Функция сохраняет данные в нужные файлы и отправляет их до пользователя, который
        тригернул ее через ТГ бота

        :param bot_token: токен бота, через которого будут отправляться файлы

        :param task_ids:
        """
        bot = telebot.TeleBot(bot_token)

        list_file_name = kwargs['ti'].xcom_pull(task_ids=task_ids)
        dict_variables = kwargs['ti'].xcom_pull(task_ids='task_get_variables')

        # Отправляем файлы пользователю
        with ExitStack() as stack:
            files_open = [stack.enter_context(open(file, 'rb')) for file in list_file_name]
            list_media_group = [
                InputMediaDocument(file_open, caption=f"Отчет по количеству справок за период с {dict_variables['date_start']} по {dict_variables['date_end']}")
                if i == len(files_open) - 1 else InputMediaDocument(file_open) for i, file_open in enumerate(files_open)]

            bot.send_media_group(kwargs["dag_run"].conf['chat_id'], list_media_group)


