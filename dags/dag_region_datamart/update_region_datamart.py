from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from plugin_region_datamart.plugin_region_datamart_update import Update_region_datamart
from plugin_region_datamart.main_sql_scripts import (year_and_month_sql, comparable_script, fish_8_script, list_rank_table_scripts,
                                                     list_name_rank_table)

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

with DAG(dag_id='test_etl_region_update_datamart',
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=50),
         tags=['TEST']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Тест Витрина Регионов РФ'",
        postgres_conn_id='')

    # Очистака промежуточной таблицы
    truncate_median_table = PythonOperator(task_id='truncate_median_table',
                                           python_callable=class_instance.truncate_median_table,
                                           op_kwargs={'table_name': 'update_region_datamart'}
                                           )

    # TaskGroup отвечающая за обновление промежуточной таблицы (месячные и годовые данные)
    with TaskGroup(group_id='update_year_and_month_data') as update_year_and_month_data:
        for year in range(2018, 2025):
            task_update_table = PythonOperator(task_id=f'task_update_table_{year}',
                                    python_callable=class_instance.update_median_table,
                                    op_kwargs={'year': year,
                                               'sq_main': year_and_month_sql,
                                               'need_table': 'update_region_datamart'},
                                    pool='year_data_update')

    task_update_comparable_data = PythonOperator(task_id='task_update_comparable_data',
                                                 python_callable=class_instance.update_data_comparable,
                                                 op_kwargs={'sq_main': comparable_script,
                                                            'need_table': 'update_region_datamart'})

    task_update_fish_8 = PythonOperator(task_id='task_update_fish_8',
                                                 python_callable=class_instance.update_script_not_params,
                                                 op_kwargs={'sq_main': fish_8_script,
                                                            'need_table': 'update_region_datamart'})

    # Обновление витрины данных
    insert_datamart_task = PythonOperator(task_id='insert_datamart_task',
                                          python_callable=class_instance.insert_datamart,
                                          op_kwargs={'table_source': 'update_region_datamart',
                                                     'table_update': 'region_datamart'})



    with TaskGroup(group_id='truncate_rank_table') as truncate_rank_table:
        for table in list_name_rank_table:
            task_truncate_rank_table = PythonOperator(task_id=f'truncate_{table}',
                                                 python_callable=class_instance.truncate_median_table,
                                                 op_kwargs={'table_name': table}
                                                 )

    with TaskGroup(group_id='update_rank_table') as update_rank_table:
        for table, script in zip(list_name_rank_table, list_rank_table_scripts):
            task_update_rank_table = PythonOperator(task_id=f'update_{table}',
                                            python_callable=class_instance.update_script_not_params,
                                            op_kwargs={'sq_main': script,
                                                       'need_table': table})
            list_id_task.append(task_update_rank_table.task_id)

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': list_id_task,
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 19,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 19,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    timeout_operation_update >> truncate_median_table >> update_year_and_month_data
    update_year_and_month_data >> [task_update_comparable_data, task_update_fish_8] >> insert_datamart_task >> truncate_rank_table >> update_rank_table
    update_rank_table >> alert_branch >> [task_s, task_f]