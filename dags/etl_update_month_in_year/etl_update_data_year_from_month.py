from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_update_year_datamart_from_month.plugin_update_year_data_from_month import Update_web_app_datamart
from plugin_update_year_datamart_from_month.sql_scripts import sq_month_by_eyar
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Update_web_app_datamart({'user': connect_hook.login,
                                          'password': connect_hook.password,
                                          'host': connect_hook.host,
                                          'port': connect_hook.port,
                                          'database': connect_hook.schema})
alert_instance = Alert_help_class(os.getenv('ETL_BOT_TOKEN'),
                                 {'user': connect_hook_postgre.login,
                                  'password': connect_hook_postgre.password,
                                  'host': connect_hook_postgre.host,
                                  'port': connect_hook_postgre.port,
                                  'database': connect_hook_postgre.schema})
# Список с id task в task_group
list_id_final_update = []

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 3, 18),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='etl_final_update_web_app_year_datamart',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(seconds=20),
         tags=['DATAMART']) as dag:
    # Обновление годовых данных из месячных
    with TaskGroup(group_id='final_update') as final_update:
        for year in [2023, 2024]:
            task_fn_update = PythonOperator(task_id=f'update_{year}',
                                            python_callable=class_instance.update_2023_year,
                                            op_kwargs={'sql_query': sq_month_by_eyar,
                                                       'year': year})
            list_id_final_update.append(task_fn_update.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_final_update,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 5,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 5,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    final_update >> alert_branch >> [task_s, task_f]
