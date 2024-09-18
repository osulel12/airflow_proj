import telebot
from airflow.utils.state import State
import psycopg2
from psycopg2.sql import Identifier, SQL
import logging
import typing
from contextlib import ExitStack
from telebot.types import InputMediaDocument
import os


class Alert_help_class:

    def __init__(self, token: str, dict_postgre_cred: dict):
        """
        :param token: Токен бота в которого будут приходить алерты

        :param dict_postgre_cred: подключение к базе данных Postgre
        """
        self.bot = telebot.TeleBot(token)

        # Подключение к базе PostgreSql
        self.psycopg2_connect = psycopg2.connect(user=dict_postgre_cred['user'],
                                               password=dict_postgre_cred['password'],
                                               host=dict_postgre_cred['host'],
                                               port=dict_postgre_cred['port'],
                                               database=dict_postgre_cred['database'])


    def get_need_chat_id(self, alert_id: int, name_table: str) -> list[int]:
        """
        В зависимости от переданного индентификатора алерта получаем набор чат id пользователей

        :param alert_id: уникальный идентификтора алерта

        :param name_table: название таблицы из которой берутся chat_id для переданного алерта

        :return: список с chat_id
        """
        with self.psycopg2_connect.cursor() as cur:
            cur.execute(SQL("""SELECT chat_id FROM bot.{} 
                                WHERE status_alert = True AND alert_id = %s""").format(Identifier(name_table)),
                        (alert_id,))
            list_need_chat_id = [chat_id[0] for chat_id in cur.fetchall()]
            logging.info(list_need_chat_id)

        return list_need_chat_id

    def get_type_alert(self, alert_id: int, name_table: str) -> str:
        """
        :param alert_id: id алерта

        :param name_table: название таблицы, где указаны названия алертов

        :return: название алерта для оповещения
        """
        with self.psycopg2_connect.cursor() as cur:
            cur.execute(SQL("""SELECT type_alert FROM bot.{} WHERE alert_id = %s""").format(Identifier(name_table)),
                        (alert_id,))
            type_alert = cur.fetchone()[0]
            logging.info(type_alert)

        return type_alert

    @staticmethod
    def branch_func(value_task_id: str | list, name_task_s: str, name_task_f: str, dag_run, **kwargs) -> str:
        """
        Реализация логики оператора ветвления

        :param value_task_id: список с id тасок или id таска после которого будет идти ветвление

        :param name_task_s: имя таска, который будет вызван при успешном завершении предыдущего таска

        :param name_task_f: имя таска, который будет вызван при не удачном завершении предыдущего таска

        :param dag_run: экземпляр запущенного dag run

        :param kwargs: дополнительные параметры

        :return: id следующего таска, который необходимо выполнить
        """
        if isinstance(value_task_id, list):
            flag_task_status = len([i for i in value_task_id if dag_run.get_task_instance(i).state != State.SUCCESS]) == 0
        else:
            flag_task_status = dag_run.get_task_instance(value_task_id).state == State.SUCCESS

        if flag_task_status:
            return name_task_s
        return name_task_f

    def alert_success(self, alert_id: int, name_table_alert_status: str, name_table_alert: str,
                      task_ids: typing.Optional[str] = 'False', del_file_flag: typing.Optional[bool] = False,
                      **kwargs):
        """
        Алерт при успешной отработки задачи

        :param alert_id: id алерта, о котором нужно оповестить

        :param name_table_alert_status: название таблицы, где указаны подписки пользователей на алерты

        :param name_table_alert: название таблицы, где указаны названия алертов

        :param del_file_flag: флаг, от которого зависит удалять или не удалять файлы

        :param task_ids: id таска из которого мы забираем список путей к файлам
        """
        type_alert = self.get_type_alert(alert_id, name_table_alert)
        for chat in self.get_need_chat_id(alert_id, name_table_alert_status):
            if task_ids != 'False':
                send_file_list = kwargs['ti'].xcom_pull(task_ids=task_ids)
                logging.info((task_ids, send_file_list))
                if send_file_list:
                    # Открываем необходимые файлы с помощью ExitStack для отправки
                    with ExitStack() as stack:
                        files_open = [stack.enter_context(open(file, 'rb')) for file in send_file_list]
                        list_media_group = [
                            InputMediaDocument(file_open, caption=f"✅ {type_alert}") if i == len(
                                files_open) - 1 else InputMediaDocument(file_open)
                            for i, file_open in enumerate(files_open)]
                        self.bot.send_media_group(chat, list_media_group)
                else:
                    self.bot.send_message(chat_id=chat,
                                      text=f"✅ {type_alert}")
            else:
                self.bot.send_message(chat_id=chat,
                                      text=f"✅ {type_alert}")
        if del_file_flag:
            for file_close in send_file_list:
                if os.path.exists(file_close):
                    os.remove(file_close)

    def alert_failed(self, alert_id: int, name_table_alert_status: str, name_table_alert: str):
        """
        Алерт при не удачной отработки задачи

        :param alert_id: id алерта, о котором нужно оповестить

        :param name_table_alert_status: название таблицы, где указаны подписки пользователей на алерты

        :param name_table_alert: название таблицы, где указаны названия алертов
        """
        type_alert = self.get_type_alert(alert_id, name_table_alert)

        for chat in self.get_need_chat_id(alert_id, name_table_alert_status):
            self.bot.send_message(chat_id=chat,
                                  text=f"⛔️ {type_alert}")