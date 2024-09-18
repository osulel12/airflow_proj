from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_ref_update.plugin_ref import Update_ref_table
from plugin_ref_update.sql_query_ref import list_ref_sql
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Update_ref_table({'user': connect_hook.login,
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
                                  'database': connect_hook_postgre.schema})
# Список с id task в task_group
list_id_ref_group_update = []

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 3, 15),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='etl_ref_table_update',
         schedule_interval='30 19 * * 3',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=1),
         catchup=False,
         tags=['DATAMART']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Витрина Month_Data'",
        postgres_conn_id='')

    # Обновляем справочники
    with TaskGroup(group_id='ref_group_update') as ref_group_update:
        for sql, table in zip(list_ref_sql, ['ref_fao_product', 'ref_tnved_postgres']):
            task_update_ref = PythonOperator(
                task_id=table,
                python_callable=class_instance.run_scripts,
                op_kwargs={'sql_script': sql,
                           'name_clickhouse_table': table}
            )
            # Получаем id каждого таска в группе
            list_id_ref_group_update.append(task_update_ref.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_ref_group_update,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 7,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 7,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    # Триггерим основной DAG обновления месячных данных
    trigger_update_month_datamart_dag = TriggerDagRunOperator(task_id='trigger_update_month_datamart_dag',
                                                              trigger_dag_id='etl_month_datamart_update')

    timeout_operation_update >> ref_group_update >> alert_branch >> [task_s, task_f]
    task_s >> trigger_update_month_datamart_dag
