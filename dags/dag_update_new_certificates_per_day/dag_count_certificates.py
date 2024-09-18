"""
Даг подсчитывает количество справок и высылает отчет пользователю
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from update_new_certificates_plugin.main_scripts import Update_new_certificates_date

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

connect_hook = BaseHook.get_connection('')
class_instance = Update_new_certificates_date({'user': connect_hook.login,
                                               'password': connect_hook.password,
                                               'host': connect_hook.host,
                                               'port': connect_hook.port,
                                               'database': connect_hook.schema})

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 8, 7),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='count_certificates_in_folder',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['certificates']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Отчет-Количество Справок'",
        postgres_conn_id='')

    # Получаем параметры временного интервала
    task_get_variables = PythonOperator(task_id='task_get_variables',
                                        python_callable=class_instance.get_variables,
                                        op_kwargs={'operation_name': 'Отчет-Количество Справок'})

    # Создаем файлы отчетности
    task_create_report_file = PythonOperator(task_id='task_create_report_file',
                                             python_callable=class_instance.create_file_report_count_certificates,
                                             op_kwargs={'path_list_reference': [r'',
                                                                      r''],
                                                        'path_potential': r'',
                                                        'path_non_standard': r''})

    # Отправляем файлы пользователю, который триггерил даг
    task_send_report_from_user = PythonOperator(task_id='task_send_report_from_user',
                                                python_callable=class_instance.get_file_report_from_user,
                                                op_kwargs={'bot_token': os.getenv('ETL_BOT_TOKEN'),
                                                           'task_ids': 'task_create_report_file'})

    timeout_operation_update >> task_get_variables >> task_create_report_file >> task_send_report_from_user

