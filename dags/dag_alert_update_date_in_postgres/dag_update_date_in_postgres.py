"""
Алертинг пользователей о добавленных данных в Postgre
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from plugin_update_date_in_postgres.plugin_dag_update_date_in_postgres import Alert_update_data_in_postgres

# Подгружаем токены и пароли
dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# Получаем нужные connection и инициализируем экземпляр класса
connect_hook = BaseHook.get_connection('')
connect_hook_postgre = BaseHook.get_connection('')
class_instance = Alert_update_data_in_postgres({'user': connect_hook.login,
                                                'password': connect_hook.password,
                                                'host': connect_hook.host,
                                                'port': connect_hook.port,
                                                'database': connect_hook.schema},
                                               {'user': connect_hook_postgre.login,
                                                'password': connect_hook_postgre.password,
                                                'host': connect_hook_postgre.host,
                                                'port': connect_hook_postgre.port,
                                                'database': connect_hook_postgre.schema},
                                               os.getenv('OUTER_BOT_TOKEN'))

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'priority_weight': 10,
    'start_date': datetime(2024, 7, 3),
    'end_date': datetime(2099, 1, 1),
    'trigger_rule': 'all_success'
}

with DAG(dag_id='alert_update_data_in_postgres',
         schedule_interval='0 6 * * 1,2,3,4,5,6,7',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=70),
         catchup=False,
         tags=['ALERT']) as dag:
    # {{ (logical_date + macros.datetime(hour=3)).strftime('%Y-%m-%d') }}
    # {{ logical_date| ds }}
    # Сенсор проверики наличия новых данных, проверяется как логическая дата запуска DAG минус день обновлени данных
    # под логической датой запуска продразумевается день, за который мы запустили DAG
    sensor_year_data = SqlSensor(task_id='sensor_year_data',
                                 sql="""SELECT name_rus FROM ss.sources_updates 
                                        WHERE '{{ macros.datetime.now().date() }}' - c_upd = 1 
                                            OR '{{ macros.datetime.now().date() }}' - i_upd = 1 
                                            OR '{{ macros.datetime.now().date() }}' - m_upd = 1""",
                                 conn_id='',
                                 timeout=600,
                                 poke_interval=60,
                                 mode='reschedule',
                                 soft_fail=True
                                 )

    # Ветвление в зависимости от наличия данных
    alert_branch_year = BranchPythonOperator(task_id='alert_branch_year',
                                             python_callable=class_instance.branch_func,
                                             trigger_rule='all_done',
                                             op_kwargs={'t_id': 'sensor_year_data',
                                                        'name_s': 'create_year_alert',
                                                        'name_f': 'task_f_year'})
    # Создаем словарь с добавленными годовыми данными
    create_year_alert = PythonOperator(task_id='create_year_alert',
                                       python_callable=class_instance.year_update)
    # Алерт при наличии данных
    task_s_year = PythonOperator(task_id='task_s_year',
                                 python_callable=class_instance.alert_success,
                                 op_kwargs={'task_ids': 'create_year_alert',
                                            'key_xcom': 'year_update'})
    # Алерт при отсутствии данных
    task_f_year = PythonOperator(task_id='task_f_year',
                                 python_callable=class_instance.alert_failed,
                                 op_kwargs={'flag_branch': 'year'}
                                 )

    sensor_year_data >> alert_branch_year >> [create_year_alert, task_f_year]
    create_year_alert >> task_s_year

    # Сенсор проверики наличия новых данных, проверяется как логическая дата запуска DAG минус день обновлени данных
    # под логической датой запуска продразумевается день, за который мы запустили DAG
    sensor_month_data = SqlSensor(task_id='sensor_month_data',
                                  sql="""SELECT name_rus FROM ss.sources_updates_m 
                                            WHERE '{{ macros.datetime.now().date() }}' - c_upd = 1 
                                                OR '{{ macros.datetime.now().date() }}' - i_upd = 1 
                                                OR '{{ macros.datetime.now().date() }}' - m_upd = 1""",
                                  conn_id='',
                                  timeout=600,
                                  poke_interval=60,
                                  mode='reschedule',
                                  soft_fail=True
                                  )

    # Ветвление в зависимости от наличия данных
    alert_branch_month = BranchPythonOperator(task_id='alert_branch_month',
                                              python_callable=class_instance.branch_func,
                                              trigger_rule='all_done',
                                              op_kwargs={'t_id': 'sensor_month_data',
                                                         'name_s': 'create_month_alert',
                                                         'name_f': 'task_f_month'})
    # Создаем словарь с добавленными месячными данными
    create_month_alert = PythonOperator(task_id='create_month_alert',
                                        python_callable=class_instance.month_update)
    # Алерт при наличии данных
    task_s_month = PythonOperator(task_id='task_s_month',
                                  python_callable=class_instance.alert_success,
                                  op_kwargs={'task_ids': 'create_month_alert',
                                             'key_xcom': 'month_update'})
    # Алерт при отсутствии данных
    task_f_month = PythonOperator(task_id='task_f_month',
                                  python_callable=class_instance.alert_failed,
                                  op_kwargs={'flag_branch': 'month'})

    sensor_month_data >> alert_branch_month >> [create_month_alert, task_f_month]
    create_month_alert >> task_s_month