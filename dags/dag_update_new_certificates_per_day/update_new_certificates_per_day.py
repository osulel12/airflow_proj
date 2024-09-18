"""
Даг собирает даты самых актуальных справок в каждой из директории и отправляет в виде файла пользователю
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

with DAG(dag_id='update_certificates_date',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['certificates']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Актуальные Даты Справок'",
        postgres_conn_id='')

    # Зачищаем таблицу
    truncate_latest_version_files = PostgresOperator(task_id='truncate_latest_version_files',
                                                     sql="""TRUNCATE TABLE tl.latest_version_files""",
                                                     postgres_conn_id='',
                                                     autocommit=True
                                                     )

    vacuum_latest_version_files = PostgresOperator(task_id='vacuum_latest_version_files',
                                                   sql=f"""VACUUM (FULL, ANALYZE) tl.latest_version_files""",
                                                   postgres_conn_id='',
                                                   autocommit=True
                                                   )

    # Обновляем даты для стран и групп стран
    update_date_group_coutry = PythonOperator(task_id='update_date_group_coutry',
                                              python_callable=class_instance.insert_db_current_creation_date_file,
                                              op_kwargs={'path': r'',
                                                         'country_or_group': 'group'})

    update_date_coutry = PythonOperator(task_id='update_date_coutry',
                                        python_callable=class_instance.insert_db_current_creation_date_file,
                                        op_kwargs={'path': r'',
                                                   'country_or_group': 'country'})

    send_document_from_user = PythonOperator(task_id='send_document_from_user',
                                             python_callable=class_instance.get_file_from_user,
                                             op_kwargs={'bot_token': os.getenv('ETL_BOT_TOKEN')})

    timeout_operation_update >> truncate_latest_version_files >> vacuum_latest_version_files >> [update_date_coutry, update_date_group_coutry]
    [update_date_coutry, update_date_group_coutry] >> send_document_from_user
