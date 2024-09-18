from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_update_tg_bot_datamart.plugin_outer_tg_bot import Update_outer_tg_bot_datamart
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
connect_hook_postgre_mh = BaseHook.get_connection('')
class_instance = Update_outer_tg_bot_datamart(
                        dict_click_cred={'user': connect_hook.login,
                                         'password': connect_hook.password,
                                         'host': connect_hook.host,
                                         'port': connect_hook.port,
                                         'database': connect_hook.schema},
                        dict_postgre_cred={'user': connect_hook_postgre.login,
                                           'password': connect_hook_postgre.password,
                                           'host': connect_hook_postgre.host,
                                           'port': connect_hook_postgre.port,
                                           'database': connect_hook_postgre.schema},
                        dict_postgre_mh={'user': connect_hook_postgre_mh.login,
                                         'password': connect_hook_postgre_mh.password,
                                         'host': connect_hook_postgre_mh.host,
                                         'port': connect_hook_postgre_mh.port,
                                         'database': connect_hook_postgre_mh.schema})
alert_instance = Alert_help_class(os.getenv('ETL_BOT_TOKEN'),
                                 {'user': connect_hook_postgre.login,
                                  'password': connect_hook_postgre.password,
                                  'host': connect_hook_postgre.host,
                                  'port': connect_hook_postgre.port,
                                  'database': connect_hook_postgre.schema}
                                 )


DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 9, 11),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='update_outer_tg_bot_datamart',
         schedule_interval='0 22 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['DATAMART']) as dag:
    # Проверяем параметр переданный при запуске
    check_trigger_params = PythonOperator(task_id='check_trigger_params',
                                          python_callable=class_instance.check_trigger_params)

    # Обновляем таблицу
    update_datamart = PythonOperator(task_id='update_datamart',
                                     python_callable=class_instance.update_datamart_outer_bot,
                                     op_kwargs={'name_table': 'outer_tg_bot_datamart'})

    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Витрина outer_tg_bot'",
        postgres_conn_id='')

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': 'timeout_operation_update',
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 20,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 20,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    check_trigger_params >> update_datamart >> timeout_operation_update >> alert_branch >> [task_s, task_f]