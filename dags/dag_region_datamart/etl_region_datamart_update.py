from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_region_datamart.plugin_region_datamart_update import Update_region_datamart
from plugin_region_datamart.sql_codes_region import (sql_script_region_coords, sql_script_macro_region,
                                                     sql_update_main_table, sql_update_chart_product,
                                                     sql_update_speed, sql_update_need)
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Update_region_datamart({'user': connect_hook.login,
                                         'password': connect_hook.password,
                                         'host': connect_hook.host,
                                         'port': connect_hook.port,
                                         'database': connect_hook.schema},
                                        {'user': connect_hook_postgre.login,
                                         'password': connect_hook_postgre.password,
                                         'host': connect_hook_postgre.host,
                                         'port': connect_hook_postgre.port,
                                         'database': connect_hook_postgre.schema})
alert_instance = Alert_help_class(os.getenv('ETL_BOT_TOKEN'),
                                 {'user': connect_hook_postgre.login,
                                  'password': connect_hook_postgre.password,
                                  'host': connect_hook_postgre.host,
                                  'port': connect_hook_postgre.port,
                                  'database': connect_hook_postgre.schema}
                                 )

list_id_task = []

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 3, 11),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='etl_region_update_datamart',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=10),
         tags=['DATAMART']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Витрина Регионов РФ'",
        postgres_conn_id='')

    task_update_main_datamart = PythonOperator(task_id='task_update_main_datamart',
                                               python_callable=class_instance.update_region_datamart,
                                               op_kwargs={'sql_datamart': sql_update_main_table,
                                                          'table_name_mart': 'region_main_table',
                                                          'sql_code_list': [sql_script_region_coords,
                                                                            sql_script_macro_region],
                                                          'need_table_list': ['lense_rigion_coords',
                                                                              'macro_region_from_Varya']})
    # TaskGroup отвечающая за обновление дочерних таблиц
    with TaskGroup(group_id='group_update_child_table') as group_update_child_table:
        for table, sql_query in zip(['table_chart_product_year', 'test_coords_speed', 'test_need_coords'],
                                    [sql_update_chart_product, sql_update_speed, sql_update_need]):
            ck_ref = PythonOperator(task_id=f'task{table}',
                                    python_callable=class_instance.update_table_by_insert,
                                    op_kwargs={'script': sql_query,
                                               'table_name': table})
            list_id_task.append(ck_ref.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_task,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 3,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 3,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    timeout_operation_update >> task_update_main_datamart >> group_update_child_table >> alert_branch >> [task_s, task_f]
