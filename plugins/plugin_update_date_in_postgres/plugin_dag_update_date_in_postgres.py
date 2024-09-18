import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import telebot
from telebot import util
from airflow.utils.state import State


class Alert_update_data_in_postgres:
    """
    Класс основной логики алертинга добаления новых данных
    """

    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict, bot_token: str):
        """
        :param dict_click_cred: словарь с подключением к ClickHouse
        :param dict_postgre_cred: словарь с подключением к PostgreSql
        :param bot_token: токен телеграмм бота, который будет отправлять алерты
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

        self.bot_token = bot_token

    @staticmethod
    def sourse_write(list_date: list[datetime.date, None], logical_date: datetime.date) -> str:
        """
        Функция принимающая на вход список дат обновления источников по стране и логическую дату запуска DAG
        отбираем только те источники разность logical_date - list_date[0] == 1
        :param list_date: список с датами обновления источников или None, если такой нет
        :param logical_date: логическая дата запуска DAG
        :return: строка стостоящая из списка подходящих источников
        """
        dict_need_source = {source: logical_date - date for date, source in
                            zip(list_date, ['comtrade', 'itc', 'customs'])
                            if date is not None}

        return ', '.join([k for k, v in dict_need_source.items() if v == timedelta(days=1)])

    def year_update(self, **kwargs):
        """
        # Функция собирает словарь с подходящими датами по странам для Годовых данных

        ## Пример словаря:
        {
            'Демократическая Республика Конго*2015': 'itc',
            'Доминикана*2016': 'itc'
        }

        :param kwargs: набор параметров запущенного Dag run
        :return: фактически ничего, но записывает в xcom полученный словарь
        """
        dict_json = {}
        # logical_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').date()
        date_now = datetime.now().date()
        df_year_update = pd.read_sql(f"""SELECT name_rus, year, c_upd, i_upd, m_upd 
                                                 FROM ss.sources_updates 
                                                 WHERE '{date_now}' - c_upd = 1 
                                                 OR '{date_now}' - i_upd = 1 
                                                 OR '{date_now}' - m_upd = 1""", con=self.sqlalchemy_con,
                                     dtype={'year': 'str'})

        # Формируем набор источников
        df_year_update['source'] = df_year_update[['c_upd', 'i_upd', 'm_upd']].apply(
            lambda x: self.sourse_write(x, date_now), axis=1)
        # Формируем год по патерну *2024 для конкатенации строк и дальнейшего сплитования
        df_year_update['year'] = df_year_update.year.apply(lambda x: '*' + x)

        for country, year, source in zip(df_year_update.name_rus, df_year_update.year, df_year_update.source):
            dict_json[country + year] = source

        kwargs['ti'].xcom_push(value=dict_json, key='year_update')

    def month_update(self, **kwargs):
        """
        # Функция собирает словарь с подходящими датами по странам для Месячных данных

        ## Пример словаря:
        {
            'Германия*2024-04': 'comtrade',
            'Япония*2024-05': 'comtrade'
        }

        :param kwargs: набор параметров запущенного Dag run
        :return: фактически ничего, но записывает в xcom полученный словарь
        """
        dict_json = {}
        # logical_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').date()
        date_now = datetime.now().date()

        df_month_update = pd.read_sql(f"""SELECT name_rus, period, c_upd, i_upd, m_upd 
                                         FROM ss.sources_updates_m 
                                         WHERE '{date_now}' - c_upd = 1 
                                         OR '{date_now}' - i_upd = 1 
                                         OR '{date_now}' - m_upd = 1""", con=self.sqlalchemy_con,
                                      parse_dates={'period': '%Y-%m-%d'})

        # Формируем набор источников
        df_month_update['source'] = df_month_update[['c_upd', 'i_upd', 'm_upd']].apply(
            lambda x: self.sourse_write(x, date_now), axis=1)
        # Формируем период по патерну *2024-04 для конкатенации строк и дальнейшего сплитования
        df_month_update['period'] = df_month_update.period.apply(lambda x: '*' + x.strftime('%Y-%m'))

        for country, period, source in zip(df_month_update.name_rus, df_month_update.period, df_month_update.source):
            dict_json[country + period] = source

        kwargs['ti'].xcom_push(value=dict_json, key='month_update')

    def get_need_chat_id(self, alert_id: int) -> list[int]:
        """
        В зависимости от переданного индентификатора алерта получаем набор чат id пользователей
        :param alert_id: уникальный идентификтора алерта
        :return: список с chat_id
        """
        with self.sqlalchemy_con.cursor() as cur:
            cur.execute("""SELECT chat_id FROM bot.alert_status WHERE status_alert = True AND alert_id = %s""",
                        (alert_id,))
            list_need_chat_id = [chat_id[0] for chat_id in cur.fetchall()]
        return list_need_chat_id

    def alert_success(self, task_ids: str, key_xcom: str, **kwargs):
        """
        Алерт функция при наличии месячных или годовых данных в БД Postgre

        :param task_ids: id таска в котором собирался нужный нам словарь (год или месяц)
        :param key_xcom: ключ xcom в который был записан словарь
        :param kwargs: набор параметров dag_run
        :return:
        """
        header_text = f"🗂 Добавлены новые {'месячные' if 'month' in key_xcom else 'годовые'} данные внешней торговли:\n"
        dict_text_alert = kwargs['ti'].xcom_pull(task_ids=task_ids, key=key_xcom)
        split_alert_message = ''

        for key, source in dict_text_alert.items():
            country, date = key.split('*')
            split_alert_message += f"{country} {'за период ' + date if 'month' in key_xcom else date + ' год'}, источники: {source}\n"

        bot = telebot.TeleBot(self.bot_token)
        for chat_id in self.get_need_chat_id(2):
            # Если сообщение будет большим, то делим его на несколько сообщений
            # и конкатенируя их с header_text отправляем пользователю
            for text in util.smart_split(split_alert_message, 4000):
                text_alert = header_text + text
                bot.send_message(chat_id, text_alert)

    def alert_failed(self, flag_branch):
        """
        Алерт функция если данных не оказалось

        :param flag_branch: название ветки, в которой мы смотрим данные (Месячные данные/Годовые данные)
        :return:
        """
        bot = telebot.TeleBot(self.bot_token)
        for chat_id in self.get_need_chat_id(2):
            bot.send_message(chat_id, f"Обновленных {'месячных' if 'month' in flag_branch else 'годовых'} данных внешней торговли нет")

    @staticmethod
    def branch_func(t_id: str, name_s: str, name_f: str, dag_run) -> str:
        """
        Реализуем алгоритм ветвления, если данные есть, то возвращаем name_s, в противном случае name_f

        :param t_id: id таска от состояния которого зависит выбор ветви

        :param name_s: id таска, который будет вызван при состоянии SUCCESS

        :param name_f: id таска, который будет вызван при любом другом состоянии, кроме failed

        :param dag_run: dag_run в котором мы отслеживаем таски, чтобы получить через него их состояние

        :return: id таска
        """
        state = dag_run.get_task_instance(t_id).state
        if state == State.SUCCESS:
            return name_s
        return name_f
