from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from plugin_web_app_datamart.plugin_update_web_app_datamart import Update_web_app_datamart
from plugin_web_app_datamart.sql_scripts_web_app import (list_ref_table, list_name_id_task,
                                                         sql_script_year, sql_fish8_year)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Update_web_app_datamart({'user': connect_hook.login,
                                          'password': connect_hook.password,
                                          'host': connect_hook.host,
                                          'port': connect_hook.port,
                                          'database': connect_hook.schema},
                                         {'user': connect_hook_postgre.login,
                                          'password': connect_hook_postgre.password,
                                          'host': connect_hook_postgre.host,
                                          'port': connect_hook_postgre.port,
                                          'database': connect_hook_postgre.schema})

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

with DAG(dag_id='etl_web_app_datamart',
         schedule_interval='0 3 * * 1,2,3,4,5,6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['DATAMART']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Витрина Year_Data'",
        postgres_conn_id='')

    # TaskGroup отвечающая за создане словарей из справочников
    with TaskGroup(group_id='ref_group_app') as ref_group:
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
                                           op_kwargs={'table_name': 'update_year_data'}
                                           )
    ref_group >> truncate_median_table

    # TaskGroup отвечающая за обновление данных в промежуточной таблице
    with TaskGroup(group_id='group_update_median_table') as group_update:
        for year in [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]:
            update_tmp = PythonOperator(task_id=f"update_{year}",
                                        python_callable=class_instance.update_median_table,
                                        op_kwargs={'year': year,
                                                   'sq_main': sql_script_year,
                                                   'sq_fish': sql_fish8_year,
                                                   'need_table': 'update_year_data'},
                                        pool='year_data_update')
    # Обновление витрины данных
    insert_datamart_task = PythonOperator(task_id='insert_datamart_task',
                                          python_callable=class_instance.insert_datamart,
                                          op_kwargs={'table_source': 'update_year_data',
                                                     'table_update': 'year_app_datamart'})

    trigger_update_month_data_in_year = TriggerDagRunOperator(task_id='trigger_update_month_data_in_year',
                                                              trigger_dag_id='etl_final_update_web_app_year_datamart')

    timeout_operation_update >> truncate_median_table >> group_update >> insert_datamart_task >> trigger_update_month_data_in_year
