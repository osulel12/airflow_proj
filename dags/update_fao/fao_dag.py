"""
Процесс обновления данных выбранных тыблиц FAO
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os
from update_table_fao_and_usda_plagins.fao_plagin import mkdir_or_del_temp_load_file, sql_get_nameFile_and_table, \
    func_upload_data, loading_temporary_table
from alert_class.alert_plugin import Alert_help_class
from dotenv import load_dotenv


dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


connect_hook = BaseHook.get_connection('')
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
    'start_date': datetime(2024, 3, 1),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='fao_update_table',
         schedule_interval='00 19 1 * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=30),
         catchup=False,
         tags=['FAO']) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Обновление таблиц FAO'",
        postgres_conn_id='')

    # Создаем необходимую директорию
    create_tmp_dir = PythonOperator(
        task_id='create_tmp_dir',
        python_callable=mkdir_or_del_temp_load_file
    )

    # Получаем названия файлов, таблиц, наименования колонок
    get_name_file_and_table = PostgresOperator(
        task_id='get_name_file_and_table',
        sql=sql_get_nameFile_and_table,
        postgres_conn_id=''
    )

    # Скачивание данных из json
    upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=func_upload_data
    )

    timeout_operation_update >> [create_tmp_dir, get_name_file_and_table] >> upload_data

    # Генерируем динамически task_group, где каждая таблица обновляется отдельно
    with open('/need_m/name_table.json', 'r', encoding='utf-8') as fl:
        dct = json.load(fl)
        with TaskGroup(group_id='first_gr') as tg1:
            for table, file_ph_and_columns in dct.items():

                pg_truncate = PostgresOperator(task_id=f'truncate_{table}',
                                               sql=f"""TRUNCATE TABLE tl."{table}\"""",
                                               postgres_conn_id='',
                                               autocommit=True
                                               )

                pg_vacuum = PostgresOperator(task_id=f'vacuum_{table}',
                                             sql=f"""VACUUM (FULL, ANALYZE) tl."{table}\"""",
                                             postgres_conn_id='',
                                             autocommit=True
                                             )

                py_transform = PythonOperator(task_id=f'transform_{table}',
                                              python_callable=loading_temporary_table,
                                              op_kwargs={'table': table, 'file_ph': file_ph_and_columns},
                                              templates_dict={'user': connect_hook.login,
                                                              'password': connect_hook.password,
                                                              'host': connect_hook.host,
                                                              'port': connect_hook.port,
                                                              'database': connect_hook.schema},
                                              pool='fao_pool')

                pg_truncate_fao = PostgresOperator(task_id=f'truncate_fao_{table}',
                                                   sql=f"""TRUNCATE TABLE o."{table}\"""",
                                                   postgres_conn_id='',
                                                   autocommit=True
                                                   )

                pg_vacuum_fao = PostgresOperator(task_id=f'vacuum_fao_{table}',
                                                 sql=f"""VACUUM (FULL, ANALYZE) o."{table}\"""",
                                                 postgres_conn_id='',
                                                 autocommit=True
                                                 )

                pg_update_fao = PostgresOperator(task_id=f'update_{table}',
                                                 sql=f"""INSERT INTO fao."{table}"
                                                         SELECT * FROM tl."{table}" """,
                                                 postgres_conn_id='',
                                                 autocommit=True,
                                                 pool='update_fao'
                                                 )

                pg_truncate >> pg_vacuum >> py_transform >> pg_truncate_fao >> pg_vacuum_fao >> pg_update_fao

    # Удаляем все скаченные файлы
    del_tmp_dir = PythonOperator(
        task_id='del_tmp_dir',
        python_callable=mkdir_or_del_temp_load_file,
        op_args=['del']
    )

    # Алерт в зависимости от индикатора завершения задачи
    alert_branch = BranchPythonOperator(task_id='alert_branch',
                                        python_callable=alert_instance.branch_func,
                                        trigger_rule='all_done',
                                        op_kwargs={'value_task_id': 'del_tmp_dir',
                                                   'name_task_s': 's',
                                                   'name_task_f': 'f'})
    task_s = PythonOperator(task_id='s',
                            python_callable=alert_instance.alert_success,
                            op_kwargs={'alert_id': 8,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})
    task_f = PythonOperator(task_id='f',
                            python_callable=alert_instance.alert_failed,
                            op_kwargs={'alert_id': 8,
                                       'name_table_alert_status': 'alert_status_etl_bot',
                                       'name_table_alert': 'alert_type_table_etl_bot'})

    upload_data >> tg1 >> del_tmp_dir >> alert_branch >> [task_s, task_f]
