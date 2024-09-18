import clickhouse_connect
import pandas as pd
import logging
import psycopg2
from datetime import timedelta


class Update_outer_tg_bot_datamart:
    """
    Класс основной логики обновления витрины статистики внешнего тг бота
    """
    def __init__(self, dict_click_cred: dict, dict_postgre_cred: dict, dict_postgre_mh: dict):
        """
        :param dict_click_cred: креды подключения к ClickHouse

        :param dict_postgre_cred: креды подключения к Postgre

        :param dict_postgre_mh: креды подключения к Postgre на мастерхосте
        """
        # Подключение к базе ClickHouse TG_bot
        self.click_house_client = clickhouse_connect.get_client(host=dict_click_cred['host'],
                                                                port=dict_click_cred['port'],
                                                                username=dict_click_cred['user'],
                                                                password=dict_click_cred['password'],
                                                                database=dict_click_cred['database'])

        # Подключение к базе PostgreSql AE
        self.psycopg2_con = psycopg2.connect(user=dict_postgre_cred['user'],
                                               password=dict_postgre_cred['password'],
                                               host=dict_postgre_cred['host'],
                                               port=dict_postgre_cred['port'],
                                               database=dict_postgre_cred['database'])

        # Подключение к базе PostgreSql MH
        self.psycopg2_con_mh = psycopg2.connect(user=dict_postgre_mh['user'],
                                               password=dict_postgre_mh['password'],
                                               host=dict_postgre_mh['host'],
                                               port=dict_postgre_mh['port'],
                                               database=dict_postgre_mh['database'])

    def truncate_table(self, table_name: str):
        """
        Очистка переданной таблицы

        :param table_name: имя очищаемой таблицы
        """
        self.click_house_client.command(f"TRUNCATE TABLE IF EXISTS {table_name}")

    def get_uu_id(self) -> str:
        """
        Формурем список uu_id зарегистрированных пользователей
        формируем из него строку для передачи ее параметром в запрос к базе данных MH

        :return: строку из uu_user_id перечисленных черезя ","
        """

        with self.psycopg2_con.cursor() as cur:
            cur.execute("""SELECT uu_user_id 
                               FROM bot.user_tg_bot
                               WHERE uu_user_id IS NOT NULL""")
            list_uu_user_id = ','.join([uu_user_id[0] for uu_user_id in cur.fetchall()])

        return list_uu_user_id

    @staticmethod
    def check_trigger_params(**kwargs) -> bool:
        """
        Проверяем параметры запуска для валидации,
        если параметр = True, значит очищаем таблицу и загружаем все данные,
        если параметр = False, значит загружаем только те данные, которые появились позже последнего запуска обновления

        :param kwargs: параметры сессии dag_runs

        :return: булево значение в зависимости от параметра
        """
        try:
            if kwargs['dag_run'].conf['flag_truncate'] == 'True':
                logging.info(
                    f"Полное обновление {kwargs['dag_run'].conf['flag_truncate']}, {type(kwargs['dag_run'].conf['flag_truncate'])}")
                return True
            else:
                logging.info(
                    f"Частичное обновление {kwargs['dag_run'].conf['flag_truncate']}, {type(kwargs['dag_run'].conf['flag_truncate'])}")
                return False
        except KeyError:
            logging.info('Частичное обновление без параметров')
            return False

    def update_datamart_outer_bot(self, name_table: str, **kwargs):
        """
        Обновление витрины данных

        :param name_table: имя обновляемой таблицы

        :param kwargs: параметры сессии dag_runs

        :return:
        """

        # Получаем имеющиеся uu_id в базе пользователей бота
        list_uu_id = self.get_uu_id()
        # Получаем данные нужных пользователей
        df_need_user_mh = pd.read_sql(f"""SELECT user_id :: text, last_name, first_name, middle_name 
                                         FROM c.users 
                                         WHERE '{list_uu_id}' LIKE('%' || user_id || '%')""",
                                      con=self.psycopg2_con_mh)
        logging.info(df_need_user_mh.head(5))

        # В зависимоти от параметра обновляем полностью или нет
        if kwargs['ti'].xcom_pull(task_ids='check_trigger_params'):
            df_logging_table = pd.read_sql("""SELECT uu_user_id AS user_id, name_button, time_operation, menu_section
                                              FROM bot.logging_table_by_click_button
                                              JOIN bot.user_tg_bot USING(chat_id)""", self.psycopg2_con)
            self.truncate_table(name_table)
        else:
            df_logging_table = pd.read_sql("""SELECT uu_user_id AS user_id, name_button, time_operation, menu_section
                                                          FROM bot.logging_table_by_click_button
                                                          JOIN bot.user_tg_bot USING(chat_id)
                                                          WHERE time_operation > 
                                                          (SELECT timeout_operation FROM bot.status_operation WHERE operation_name = 'Витрина outer_tg_bot')""",
                                           self.psycopg2_con)
        df_logging_table['time_operation'] = df_logging_table['time_operation'] + timedelta(hours=3)
        df_datamart = df_logging_table.merge(df_need_user_mh, on='user_id')
        self.click_house_client.insert_df(table=name_table, df=df_datamart[['last_name', 'first_name', 'middle_name',
                                                                            'name_button', 'time_operation', 'menu_section']])

