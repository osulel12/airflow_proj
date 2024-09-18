import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from sqlalchemy import create_engine
import logging
import telebot
from telebot.types import InputMediaDocument
import os
from contextlib import ExitStack


class Download_veterinary_certificate:

    def __init__(self, dict_postgre_cred: dict, bot_token: str, main_chat_id_admin: int = 739352279):
        """
        :param dict_postgre_cred: словарь с кредами подключений к Postrgre
        :param bot_token: токен бота
        :param main_chat_id_admin: chat_id админа, который получает алерты об ошибках
        """
        self.url_fsvps = 'https://fsvps.gov.ru/ru/fsvps/importexport'
        self.start_url_download = 'https://fsvps.gov.ru'
        self.lst_ukaz = ['№ указания', 'Указание', '№№ и дата официальных указаний (распоряжений)', '№ и дата письма',
                         'Unnamed: 3']
        self.lst_word = ['Документ в формате Word', 'Ветеринарный сертификат в формате Word']
        self.months_list = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября",
                            "октября",
                            "ноября", "декабря"]
        # Подключение к базе PostgreSql
        self.sqlalchemy_con = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'
                                            .format(dict_postgre_cred['user'],
                                                    dict_postgre_cred['password'],
                                                    dict_postgre_cred['host'],
                                                    dict_postgre_cred['port'],
                                                    dict_postgre_cred['database']))
        self.bot = telebot.TeleBot(bot_token)
        self.main_chat_id_admin = main_chat_id_admin

    def rename_col(self, column: str) -> str:
        """
        Функция переименовывания названий столбцов
        :param column: название столбца
        :return: измененное название столбца, если это необходимо
        """
        if column in self.lst_ukaz:
            return '№ указания'
        elif column in self.lst_word:
            return 'Документ Word'
        else:
            return column

    def need_url_by_cert(self, c1: list[str], c2: list[str], c3: list[str]) -> str:
        """
        Функция возвращающая ссылку на ветеринарный сертификат
        :param c1: значенпе из столбца Ветеринарный сертификат
        :param c2: значенпе из столбца № указания
        :param c3: значенпе из столбца Документ Word
        :return: правильную ссылку на документ или значение из обработки ошибки
        """
        try:
            if c1 is not None and c1[1] is not None:
                return self.start_url_download + c1[1] if 'https' not in c1[1] else c1[1]
            elif c3 is not None and c3[1] is not None:
                return self.start_url_download + c3[1] if 'https' not in c3[1] else c3[1]
            elif c2 is not None and c2[1] is not None:
                return self.start_url_download + c2[1] if 'https' not in c2[1] else c2[1]
            else:
                raise ValueError('all value in columns is None')
        except ValueError:
            return 'Ссылка отсутствует'
        except TypeError:
            return 'del'

    def need_url_by_cert_not_table(self, url: str) -> str:
        """
        Проверяет ссылку на наличие полного адреса, если адрес не полный, то дополняем его

        :param url: проверяемая ссылка

        :return: доработанную ссылку
        """
        return self.start_url_download + url if 'https' not in url else url

    def get_need_src(self, **kwargs):
        """
        Собираем нужные ссылки на страны с ветеринарными сертификатами
        :param kwargs: набор параметров
        :return: два словаря в xcom, с сылками на табличные формы и с сылками на произвольные формы
        """
        dct_table_cert = {}
        dct_not_table_cert = {}

        response = requests.get(self.url_fsvps)
        response.encoding = 'utf-8'
        soup_fsvps = BeautifulSoup(response.text, 'lxml')

        # Получаем url на каждую страну
        for i in soup_fsvps.find('div', class_='view-content').find_all('a'):
            response_country = requests.get(url=i['href'])
            response_country.encoding = 'utf-8'
            temp_bs_country = BeautifulSoup(response_country.text, 'lxml')

            # Проверяем, есть ли у нее раздел экспорта
            if temp_bs_country.find('div', class_='linkholder').find('a', string='Экспорт'):
                url_export = temp_bs_country.find('div', class_='linkholder').find('a', string='Экспорт')['href']
                response_export = requests.get(url=url_export)
                response_export.encoding = 'utf-8'
                soup_export = BeautifulSoup(response_export.text, 'lxml')

                # Проверяем есть ли раздел Ветеринарные сертификаты
                if soup_export.find('a', string='Ветеринарные сертификаты'):
                    url_sert = soup_export.find('a', string='Ветеринарные сертификаты')['href']
                    response_sert = requests.get(url=url_sert).text

                    # Определяем, в табличной форме размещены сертификаты или в произвольной
                    if BeautifulSoup(response_sert, 'lxml').find('table', class_='document'):
                        dct_table_cert[i.text.strip()] = response_sert
                    else:
                        dct_not_table_cert[i.text.strip()] = response_sert
        # Записываем два словаря в xcom
        kwargs['ti'].xcom_push(value=dct_not_table_cert, key='not_table_cert')
        kwargs['ti'].xcom_push(value=dct_table_cert, key='table_cert')

    def parse_date_type(self, x: str) -> str:
        """
        Получаем дату вступления в силу сертификата, для данных в табличной форме
        :param x: строка с датой
        :return: строку даты нужного формата
        """
        dct = {self.months_list[i]: '0' + str(i + 1) if i + 1 < 10 else str(i + 1) for i in
               range(len(self.months_list))}
        if isinstance(x, str) and len(x.split(' от ')[0]) < 40:
            for i in self.months_list:
                if i in x:
                    x = x.replace(i, dct[i])

            x = 'от '.join(re.findall(r"от \d*.\d*.\d*", x)).replace('от', '').replace('ве', '').strip().replace(
                '  ', '\n').replace(' ', '.')
            if x.replace('.', '').isdigit():
                return x
            else:
                return 'Нет данных'
        else:
            return 'Нет данных'

    def parse_date_type_no_table(self, x: str) -> str:
        """
        Получаем дату вступления в силу сертификата, для данных в произвольной форме
        :param x: строка с датой
        :return: строку даты нужного формата
        """
        dct = {self.months_list[i]: '0' + str(i + 1) if i + 1 < 10 else str(i + 1) for i in
               range(len(self.months_list))}
        if isinstance(x, str):
            for i in self.months_list:
                if i in x:
                    x = x.replace(i, dct[i]).replace(' с ', ' от ')
            x = 'от '.join(re.findall(r"от \d*.\d*.\d*", x)).replace('от', '').replace('ве', '').strip().replace(
                '  ', '\n').replace(' ', '.') + ' ' + 'до '.join(re.findall(r"до \d*.\d*.\d*", x)).replace('от',
                                                                                                           '').replace(
                'ве', '').strip().replace('  ', '\n').replace(' ', '.')
            if x.replace('.', '').isdigit():
                return x
            else:
                return 'Нет данных'
        else:
            return 'Нет данных'

    def update_table_view_cert(self, **kwargs):
        """
        Обновление сертификатов в табличной форме
        :param kwargs: произвольные параметры
        :return:
        """
        dct = kwargs['ti'].xcom_pull(task_ids='get_src_task', key='table_cert')
        df_all = pd.DataFrame()
        # Получаем единое время обновления
        date_update = datetime.now().strftime("%Y-%m-%d %H:%M")
        # Проходим каждую страну
        for country, html in dct.items():
            try:
                df = pd.read_html(html, extract_links='body')[0]
                df.dropna(subset=['Ветеринарный сертификат'], inplace=True)
                df.rename(columns=self.rename_col, inplace=True)
                df['country'] = country
                df_all = pd.concat((df_all, df))
            except Exception:
                logging.exception(f'Ошибка в обработки страны {country}')
                self.bot.send_message(chat_id=self.main_chat_id_admin, text=f'Ошибка в табличных данных {country}')
        # Преобразуем данные к единому формату
        df_all['url_certificat'] = df_all.apply(lambda x: self.need_url_by_cert(x['Ветеринарный сертификат'],
                                                                                x['№ указания'],
                                                                                x['Документ Word']), axis=1)
        df_all.fillna('Нет данных', inplace=True)
        df_all = df_all[df_all['url_certificat'] != "del"]
        df_all['certificat_name'] = df_all['Ветеринарный сертификат'].apply(lambda x: list(x))
        df_all.drop_duplicates(subset=['certificat_name'], inplace=True)
        df_all['document_number'] = df_all['№ указания'].apply(
            lambda x: '\n'.join(re.findall(r'[ФК]\w*-\w*-\d/\d+', x[0])) if isinstance(x[0], str) and len(
                x[0].split(' от ')[0]) < 40 and x != 'Нет данных' else 'Нет данных')
        df_all['document_number'] = df_all['document_number'].apply(lambda x: 'Нет данных' if x == '' else x)
        df_all['date_update'] = date_update
        df_all['version_update'] = 'new'
        df_all['period'] = df_all['№ указания'].apply(lambda x: self.parse_date_type(x[0]))
        df_all[['certificat_name', 'document_number', 'period', 'country', 'url_certificat', 'date_update',
                'version_update']] \
            .to_sql('veterinary_certificates', schema='tl', con=self.sqlalchemy_con, if_exists='append',
                    index=False)

    def update_not_table_view_cert(self, **kwargs):
        """
        Обновление сертификатов в произвольной форме
        :param kwargs: произвольные параметры
        :return:
        """
        dct = kwargs['ti'].xcom_pull(task_ids='get_src_task', key='not_table_cert')
        # Получаем единое время обновления
        date_update = datetime.now().strftime("%Y-%m-%d %H:%M")
        df_all = pd.DataFrame()
        # Проходим каждую страну
        for country, html in dct.items():
            try:
                dct_out_table = {'certificat_name': [], 'document_number': [], 'period': [], 'country': [],
                                 'url_certificat': []}
                soup_dict = BeautifulSoup(html, 'lxml').find('div', class_='newsitem').find_all('p')
                for j in soup_dict:
                    if len(j.find_all('a')) >= 1:
                        dct_out_table['certificat_name'].append(j.find('a', class_='pdf').text) if j.find('a',
                                                                                                          class_='pdf') \
                            else dct_out_table['certificat_name'].append(j.find('a').text)
                        dct_out_table['url_certificat'].append(
                            self.need_url_by_cert_not_table(j.find('a', class_='pdf')['href']) if j.find('a',
                                                                                                         class_='pdf') \
                            else self.need_url_by_cert_not_table(j.find('a')['href']))
                        dct_out_table['country'].append(country)
                        dct_out_table['document_number'].append(
                            '\n'.join(re.findall(r'[ФК]\w*-\w*-\d/\d+', j.text)) if isinstance(j.text,
                                                                                               str) and '\n'.join(
                                re.findall(r'[ФК]\w*-\w*-\d/\d+', j.text)) != '' else 'Нет данных')
                        dct_out_table['period'].append(self.parse_date_type_no_table(j.text))

                        df = pd.DataFrame(dct_out_table)
                        df['period'] = df['period'].apply(lambda x: 'Нет данных' if x == ' ' or x == [] else x)
                        df['temp'] = df['certificat_name'].apply(
                            lambda x: 'delete' if x == 0 or x == 'пример заполненного сертификата' else 'need')
                df_all = pd.concat((df_all, df.query('temp != "delete"')[
                    ['certificat_name', 'document_number', 'period', 'country', 'url_certificat']]))
            except Exception:
                logging.exception(f'Ошибка в обработки страны {country}')
                self.bot.send_message(chat_id=self.main_chat_id_admin,
                                      text=f'Ошибка неструктурированных данных {country}')
        df_all['certificat_name'] = df_all.apply(lambda x: list((x.certificat_name, x.url_certificat)), axis=1)
        df_all.drop_duplicates(subset=['certificat_name'], inplace=True)
        df_all['date_update'] = date_update
        df_all['version_update'] = 'new'
        df_all.to_sql('veterinary_certificates', schema='tl', con=self.sqlalchemy_con, if_exists='append',
                      index=False)

    def get_need_chat_id(self, alert_id: int) -> list[int]:
        """
        В зависимости от переданного индентификатора алерта получаем набор чат id пользователей
        :param alert_id: уникальный идентификтора алерта
        :return: список с chat_id
        """
        with self.sqlalchemy_con.begin() as cur:
            list_id = cur.execute(
                """SELECT chat_id FROM bot.alert_status WHERE status_alert = True AND alert_id = %s""",
                (alert_id,)).fetchall()
            list_need_chat_id = [chat_id[0] for chat_id in list_id]
        return list_need_chat_id

    def send_document(self):
        """
        Рассылка сертификатов пользователям
        :return:
        """

        # Лист, куда будут записываться названия необходимых файлов
        list_file_name = []

        # Получаем все сертификаты
        df_all_cert = pd.read_sql("""SELECT certificat_name, document_number, period, country, url_certificat
                                  FROM tl.veterinary_certificates 
                                  WHERE version_update = 'new'
                                  ORDER BY country""",
                                  con=self.sqlalchemy_con)
        df_all_cert['certificat_name'] = df_all_cert['certificat_name'].apply(lambda x: x[0])
        # Записываем их в файл
        with pd.ExcelWriter('Ветеринарные сертификаты.xlsx') as writer:
            df_all_cert.to_excel(writer, sheet_name='Сертификаты', index=False, na_rep='NaN')
            for column in df_all_cert:
                column_width = max(df_all_cert[column].astype(str).map(len).max(), len(column))
                col_idx = df_all_cert.columns.get_loc(column)
                writer.sheets['Сертификаты'].set_column(col_idx, col_idx,
                                                        len(column) + 10 if column_width > 200 else column_width)
            writer.sheets['Сертификаты'].set_default_row(30)
        list_file_name.append('Ветеринарные сертификаты.xlsx')

        # Получаем новые добавленные данные
        df_add = pd.read_sql("""SELECT certificat_name, document_number, period, country, url_certificat 
                             FROM tl.change_history_certificates 
                             WHERE DATE_TRUNC('day', date_update) = DATE_TRUNC('day', current_date)
                             AND version_update = 'add'
                             ORDER BY country""",
                             con=self.sqlalchemy_con)
        df_add['certificat_name'] = df_add['certificat_name'].apply(lambda x: x[0])

        # Получаем удаленные данные
        df_del = pd.read_sql("""SELECT certificat_name, document_number, period, country, url_certificat 
                                FROM tl.change_history_certificates 
                                WHERE DATE_TRUNC('day', date_update) = DATE_TRUNC('day', current_date)
                                AND version_update = 'del'
                                ORDER BY country""",
                             con=self.sqlalchemy_con)
        df_del['certificat_name'] = df_del['certificat_name'].apply(lambda x: x[0])

        # Проверяем, есть ли добавленные и удаленные данные
        # Если есть, то создаем файлы и записываем их названия в список
        for df_iter, num, name_file in zip([df_add, df_del], [df_add.shape[0], df_del.shape[0]],
                                           ['Новые данные.xlsx', 'Удаленные данные.xlsx']):
            if num != 0:
                with pd.ExcelWriter(name_file) as writer:
                    df_iter.to_excel(writer, sheet_name='Сертификаты', index=False, na_rep='NaN')
                    for column in df_iter:
                        column_width = max(df_iter[column].astype(str).map(len).max(), len(column))
                        col_idx = df_iter.columns.get_loc(column)
                        writer.sheets['Сертификаты'].set_column(col_idx, col_idx,
                                                                len(column) + 10 if column_width > 200 else column_width)
                    writer.sheets['Сертификаты'].set_default_row(30)
                list_file_name.append(name_file)

        # Отправка файлов до пользователей
        # Для этого открываем необходимые файлы с помощью ExitStack
        for chat_id in self.get_need_chat_id(1):
            with ExitStack() as stack:
                files_open = [stack.enter_context(open(file, 'rb')) for file in list_file_name]
                list_media_group = [
                    InputMediaDocument(file_open, caption='Результат парсинга сертификатов') if i == len(
                        files_open) - 1 else InputMediaDocument(file_open)
                    for i, file_open in enumerate(files_open)]
                self.bot.send_media_group(chat_id, list_media_group)

        # Удаляем файлы
        for file_close in list_file_name:
            if os.path.exists(file_close):
                os.remove(file_close)
