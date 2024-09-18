from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_balance.plugin_update_balance_datamart import Update_balance_datamart
from plugin_balance.sql_scripts_balance import (list_ref_table, list_name_id_task,
                                                              sql_balance, usda_sql_script)
from alert_class.alert_plugin import Alert_help_class

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Update_balance_datamart({'user': connect_hook.login,
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

with DAG(dag_id='etl_balance_datamart_update',
         schedule_interval='30 19 * * 6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=2, minutes=30),
         catchup=False,
         tags=['DATAMART']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Витрина Балансов'",
        postgres_conn_id='')

    # TaskGroup отвечающая за создане словарей из справочников
    with TaskGroup(group_id='ref_group_balance') as ref_group_balance:
        for name, sql_query in zip(list_name_id_task, list_ref_table):
            ck_ref = PythonOperator(task_id=f'data_dict_{name}',
                                    python_callable=class_instance.get_ref_dict,
                                    op_kwargs={'sql_query': sql_query,
                                               'col_key': sql_query.split(' FROM ')[0].split('SELECT ')[1].split(', ')[
                                                   0],
                                               'col_value':
                                                   sql_query.split(' FROM ')[0].split('SELECT ')[1].split(', ')[1]}
                                    )
    # Очистака промежуточной таблицы
    truncate_median_table = PythonOperator(task_id='truncate_median_table',
                                           python_callable=class_instance.truncate_median_table,
                                           op_kwargs={'table_name': 'update_balance_datamart'}
                                           )

    timeout_operation_update >> ref_group_balance >> truncate_median_table
    # TaskGroup отвечающая за обновление данных в промежуточной таблице
    with TaskGroup(group_id='group_update_median_table_balance') as group_update_median_table_balance:
        for year in [2017, 2018, 2019, 2020, 2021, 2022, 2023]:
            update_tmp = PythonOperator(task_id=f"update_{year}",
                                        python_callable=class_instance.update_median_table,
                                        op_kwargs={'year': year,
                                                   'sq_fao': sql_balance,
                                                   'sq_usda': usda_sql_script,
                                                   'need_table': 'update_balance_datamart'},
                                        pool='update_median_table_pool')
    # Обновление витрины данных
    insert_datamart_task = PythonOperator(task_id='insert_datamart_task',
                                          python_callable=class_instance.insert_datamart,
                                          op_kwargs={'table_source': 'update_balance_datamart',
                                                     'table_update': 'balance_datamart'})

    truncate_median_table >> group_update_median_table_balance >> insert_datamart_task

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': 'insert_datamart_task',
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 2,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 2,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    insert_datamart_task >> alert_branch >> [task_s, task_f]
