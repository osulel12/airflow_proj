from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

from moex_plugin.moex_etl_func import Load_data_from_moex
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Load_data_from_moex({'user': connect_hook_postgre.login,
                                      'password': connect_hook_postgre.password,
                                      'host': connect_hook_postgre.host,
                                      'port': connect_hook_postgre.port,
                                      'database': connect_hook_postgre.schema},
                                     'https://iss.moex.com/iss/securities/{sec_id}.json',
                                     'https://iss.moex.com/iss/history/engines/stock/markets/index/securities/{sec_id}?from={from_date}&till={till_date}&start={step}')
alert_instance = Alert_help_class(os.getenv('ETL_BOT_TOKEN'),
                                 {'user': connect_hook_postgre.login,
                                  'password': connect_hook_postgre.password,
                                  'host': connect_hook_postgre.host,
                                  'port': connect_hook_postgre.port,
                                  'database': connect_hook_postgre.schema})
list_id_task = []

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 5, 29),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='moex_etl_proces',
         schedule_interval='40 6 * * 2,3,4,5,6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=20),
         catchup=False,
         tags=['Parser']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Парсинг MOEX'",
        postgres_conn_id='')

    task_create_xcom = PythonOperator(task_id='task_create_xcom',
                                      python_callable=class_instance.create_xcom_for_task)

    task_cleare_table = PythonOperator(task_id='task_cleare_table',
                                       python_callable=class_instance.cleare_all_data)

    with TaskGroup(group_id='moex_task_group') as moex_task_group:
        for sec_id in ['WHFOB', 'BRFOB', 'CRFOB', 'SOEXP', 'SMEXP']:
            moex_data_task = PythonOperator(task_id=f'moex_data_task_{sec_id}',
                                            python_callable=class_instance.get_data_index_stock,
                                            op_kwargs={'sec_id': sec_id})
            list_id_task.append(moex_data_task.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_task,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 1,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 1,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    timeout_operation_update >> task_create_xcom >> task_cleare_table >> moex_task_group >> alert_branch >> [task_s, task_f]
