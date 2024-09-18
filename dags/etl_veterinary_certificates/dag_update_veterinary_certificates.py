"""
DAG обновления ветеринарных сертификатов
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from plugins_veterinary_certificates.plugin_update_veterinary_certificates import Download_veterinary_certificate
from plugins_veterinary_certificates.sql_scripts_veterinary_certificates import insert_add_cert, insert_delete_cert

dotenv_path = '/env/path/.env'
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

connect_hook_postgre = BaseHook.get_connection('')
need_exs = Download_veterinary_certificate({'user': connect_hook_postgre.login,
                                            'password': connect_hook_postgre.password,
                                            'host': connect_hook_postgre.host,
                                            'port': connect_hook_postgre.port,
                                            'database': connect_hook_postgre.schema}, os.getenv('OUTER_BOT_TOKEN'))

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'priority_weight': 10,
    'start_date': datetime(2024, 3, 17),
    'end_date': datetime(2099, 3, 17),
    'trigger_rule': 'all_success',
}

with DAG(dag_id='cert_update_dag',
         schedule_interval='0 11 * * 5',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         dagrun_timeout=timedelta(minutes=10),
         tags=['Parser'],
         catchup=False) as dag:
    # Перезаписываем время запуска DAG
    timeout_operation_update = PostgresOperator(
        task_id='timeout_operation_update',
        sql="UPDATE bot.status_operation SET timeout_operation = NOW() WHERE operation_name = 'Проверка_Сертификатов'",
        postgres_conn_id='')

    # Таск риализующий сбор необходмых ссылок и разделение их на табличные и частные случаи
    get_src_task = PythonOperator(task_id='get_src_task',
                                  python_callable=need_exs.get_need_src)

    # Очищаем старые данные
    delete_old_cert = PostgresOperator(task_id='delete_old_cert',
                                       sql=f"""DELETE FROM tl.veterinary_certificates WHERE version_update = 'old'""",
                                       postgres_conn_id='',
                                       autocommit=True
                                       )
    # Для оставшихся данных меняем значение параметра version_update
    version_update_task = PostgresOperator(task_id='version_update_task',
                                           sql=f"""UPDATE tl.veterinary_certificates
                                                       SET version_update = 'old'""",
                                           postgres_conn_id='',
                                           autocommit=True
                                           )
    # Загрузка сертификатов имеющих табличный формат
    upload_certificate_table = PythonOperator(task_id='upload_certificate_table',
                                              python_callable=need_exs.update_table_view_cert)

    # Загрузка сертификатов имеющих произвольный формат
    upload_certificate_not_table = PythonOperator(task_id='upload_certificate_not_table',
                                                  python_callable=need_exs.update_not_table_view_cert)

    # Добавление новых появившихся сертификатов
    upload_add_certificate = PostgresOperator(task_id='upload_add_certificate',
                                              sql=insert_add_cert,
                                              postgres_conn_id='',
                                              autocommit=True
                                              )

    # Добавление удаленных сертификатов
    upload_del_certificate = PostgresOperator(task_id='upload_del_certificate',
                                              sql=insert_delete_cert,
                                              postgres_conn_id='',
                                              autocommit=True
                                              )

    # Отправка новых сертификатов пользователям
    send_document_user = PythonOperator(task_id='send_document_user',
                                        python_callable=need_exs.send_document)

    timeout_operation_update >> get_src_task >> delete_old_cert >> version_update_task
    version_update_task >> [upload_certificate_table, upload_certificate_not_table]
    upload_certificate_table >> [upload_add_certificate, upload_del_certificate]
    upload_certificate_not_table >> [upload_add_certificate, upload_del_certificate]
    [upload_add_certificate, upload_del_certificate] >> send_document_user

