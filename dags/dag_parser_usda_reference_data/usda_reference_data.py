from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from alert_class.alert_plugin import Alert_help_class
from plugin_usda_reference_data.usda_reference_data import Load_usda_reference_data
from plugin_usda_reference_data.sql_scripts import list_sql_query, list_name_id_task

list_id_task_code = []

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


connect_hook = BaseHook.get_connection('')
class_instance = Load_usda_reference_data({'user': connect_hook.login,
                                  'password': connect_hook.password,
                                  'host': connect_hook.host,
                                  'port': connect_hook.port,
                                  'database': connect_hook.schema},
                                  'https://apps.fas.usda.gov/PSDOnlineApi/api/downloadableData/GetAvailabilityByCommodity?commodityCode={code}',
                                  'https://apps.fas.usda.gov/PSDOnlineApi/api/downloadableData/GetAllCommodities',
                                   os.getenv('ETL_BOT_TOKEN'))
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

with DAG(dag_id='usda_update_reference_data',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['USDA']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Обновление таблицы reference_data'",
        postgres_conn_id='')

    # Формируем справочники
    with TaskGroup(group_id='get_ref_data') as get_ref_data:
        for name, sql_query in zip(list_name_id_task, list_sql_query):
            ck_ref = PythonOperator(task_id=f'data_dict_{name}',
                                    python_callable=class_instance.get_ref_dict,
                                    op_kwargs={'sql_query': sql_query,
                                               'col_key': sql_query.split(' FROM ')[0].split('SELECT ')[1].split(', ')[
                                                   0],
                                               'col_value':
                                                   sql_query.split(' FROM ')[0].split('SELECT ')[1].split(', ')[1]})

    # Получаем коды USDA
    task_update_code_in_file = PythonOperator(task_id='task_update_code_in_file',
                               python_callable=class_instance.get_commodity_code_usda)

    # Зачищаем таблицу
    truncate_reference_data = PostgresOperator(task_id='truncate_reference_data',
                                   sql="""TRUNCATE TABLE u.reference_data""",
                                   postgres_conn_id='',
                                   autocommit=True
                                   )

    vacuum_reference_data = PostgresOperator(task_id='vacuum_reference_data',
                                 sql=f"""VACUUM (FULL, ANALYZE) u.reference_data""",
                                 postgres_conn_id='',
                                 autocommit=True
                                 )

    # Добавляем новые данные в таблицу
    with TaskGroup(group_id='update_code_data_group') as update_code_data_group:
        for code in class_instance.get_list_code_in_task_group():
            update_code = PythonOperator(task_id=f'code_{code}',
                                    python_callable=class_instance.load_data_in_db,
                                    op_kwargs={'code': code})
            # Получаем id каждого таска в группе
            list_id_task_code.append(update_code.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_task_code,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 10,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 10,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    timeout_operation_update >> [get_ref_data, task_update_code_in_file] >> truncate_reference_data >> vacuum_reference_data
    vacuum_reference_data >> update_code_data_group >> alert_branch >> [task_s, task_f]