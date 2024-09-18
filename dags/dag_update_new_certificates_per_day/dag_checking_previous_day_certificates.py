"""
Даг проверяет наличие новых справок относительно последней даты запуска и высылает отчет пользователям
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from alert_class.alert_plugin import Alert_help_class
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

alert_instance = Alert_help_class(os.getenv('ETL_BOT_TOKEN'),
                                  {'user': connect_hook.login,
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

with DAG(dag_id='checking_the_previous_day_certificates',
         schedule_interval='00 6 * * 1,2,3,4,5',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['certificates']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Новые справки с момента последнего запуска'",
        postgres_conn_id='')

    task_get_key_value = PythonOperator(task_id='task_get_key_value',
                                        python_callable=class_instance.get_key_value_in_db)

    # Обновляем даты для стран и групп стран
    checking_date_group_coutry = PythonOperator(task_id='checking_date_group_coutry',
                                                python_callable=class_instance.insert_db_checking_the_previous_day_certificates,
                                                op_kwargs={
                                                    'path': r'',
                                                    'country_or_group': 'group'})

    checking_date_coutry = PythonOperator(task_id='checking_date_coutry',
                                        python_callable=class_instance.insert_db_checking_the_previous_day_certificates,
                                        op_kwargs={'path': r'',
                                                   'country_or_group': 'country'})
    data_save_in_file = PythonOperator(task_id='data_save_in_file',
                                       python_callable=class_instance.get_actual_certificates_file_per_day)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': 'data_save_in_file',
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    # "'{{ ti.xcom_pull(task_ids='data_save_in_file')}}'" использование xcom в шаблонизаторе
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 11,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot',
                                       'task_ids': 'data_save_in_file'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 11,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    task_get_key_value >> [checking_date_group_coutry, checking_date_coutry] >> data_save_in_file
    data_save_in_file >> alert_branch >> [task_s, task_f]