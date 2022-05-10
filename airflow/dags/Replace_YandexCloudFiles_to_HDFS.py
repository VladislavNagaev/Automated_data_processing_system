from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.providers.telegram.operators.telegram import TelegramOperator
from kafka import KafkaProducer
# from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator


import datetime

import os
from glob import glob
import logging


hdfs_host = Variable.get('HDFS_host')
hdfs_port = Variable.get('HDFS_port')
hdfs_web_port = Variable.get('HDFS_web_port')

kafka_host = Variable.get('Kafka_host')
kafka_port = Variable.get('Kafka_port')
kafka_topic_user_messages = Variable.get('Kafka_topic_user_messages')
kafka_topic_admin_messages = Variable.get('Kafka_topic_admin_messages')

# Директория файловой системы (корневая папка) airflow и hadoop контейнеров
default_directory = FSHook(conn_id='fs_default').get_path()

source_working_directory = Variable.get('YandexCloud_UWCA')
source_data_folder = Variable.get('UWCA_source_csv_folder')
source_data_format = 'csv'

target_working_directory = Variable.get('HDFS_UWCA')
target_data_folder = Variable.get('HDFS_source_csv_folder')
target_data_format = 'csv'

# glob-шаблон для отслеживания файлов в исходной директории
source_glob = os.path.join(default_directory, source_working_directory, source_data_folder + '/*.' + source_data_format)


def get_file_directories(**kwargs):
    """
    Returns a list of file directories
    """

    ti = kwargs.get('ti')
    source_glob = kwargs.get('source_glob')

    # Files sorted by creation date
    path_to_files = sorted(glob(source_glob), key=os.path.getmtime)
    path_to_file = path_to_files[0]

    ti.xcom_push(key='path_to_file', value=path_to_file)


def correct_title(file_name: str):
    """
    Prepares the file name to a valid format
    """
    file_name_corr = file_name.strip().replace('  ', '_').replace('  ', '_').replace(' ', '_')
    return file_name_corr


def prepare_save_path(**kwargs):

    ti = kwargs.get('ti')
    path_to_file = ti.xcom_pull(key='path_to_file')

    # Файл
    file = os.path.basename(path_to_file)
    # Имя файла
    file_name = ''.join(os.path.splitext(file)[:-1])
    # Формат файла
    file_format = ''.join(os.path.splitext(file)[-1:])
    # Базовая директория файла
    file_base_path = os.path.dirname(path_to_file)

    # Удаление лишиних символов в наименовании
    file_name_corr = correct_title(file_name)
    # Директория сохранения файла
    path_to_target_file = os.path.join(target_working_directory, target_data_folder, file_name_corr + '.' + target_data_format)

    ti.xcom_push(key='file', value=file)
    ti.xcom_push(key='path_to_source_file', value=path_to_file)
    ti.xcom_push(key='path_to_target_file', value=path_to_target_file)
    ti.xcom_push(key='file_old_name', value=file_name)
    ti.xcom_push(key='file_new_name', value=file_name_corr)


def send_kafka_user_message(**kwargs):

    ti = kwargs.get('ti')
    file = ti.xcom_pull(key='file')
    file_old_name = ti.xcom_pull(key='file_old_name')
    file_new_name = ti.xcom_pull(key='file_new_name')
    kafka_host = kwargs.get('kafka_host')
    kafka_port = kwargs.get('kafka_port')
    kafka_topic_user_messages = kwargs.get('kafka_topic_user_messages')

    # Корректировка наименования файла
    file_rename_status = file_old_name != file_new_name

    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}')

    if file_rename_status:
        message1 = f'Файл «{file}» содержал недопустимые символы в наименовании и был переименован в «{file_new_name}.{target_data_format}»!'
        producer.send(topic=kafka_topic_user_messages, value=message1.encode('UTF-8'))

    message2 = f'Файл «{file_new_name}.{target_data_format}» был сохранен в файловой системе HDFS и удален из исходной директории Yandex.Cloud!'
    producer.send(topic=kafka_topic_user_messages, value=message2.encode('UTF-8'))


with DAG(
    dag_id="Replace_YandexCloudFiles_to_HDFS",
    start_date=datetime.datetime(2022, 4, 9, 0, 30, 0),
    schedule_interval='*/1 * * * *',
    catchup=False,
) as dag:

    waiting_for_file = FileSensor(
        task_id='waiting_for_file',
        filepath=source_glob,
        fs_conn_id='fs_default',
        recursive=True,
        poke_interval=5,
        timeout=60*1,
        mode='reschedule',
        soft_fail=True,
    )

    get_file_directories_task = PythonOperator(
        task_id='get_file_directories',
        python_callable=get_file_directories,
        op_kwargs={
            'source_glob': source_glob,
        },
        provide_context=True,
    )

    prepare_save_path_task = PythonOperator(
        task_id='prepare_save_path',
        python_callable=prepare_save_path,
        provide_context=True,
    )

    save_file_to_hdfs_task = BashOperator(
        task_id='save_file_to_hdfs',
        bash_command='curl -i -X PUT -L -T "$path_to_source_file" "$hdfs_host:$hdfs_web_port/webhdfs/v1$path_to_target_file?op=CREATE&overwrite=true";',
        env={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            'hdfs_host': hdfs_host,
            'hdfs_web_port': hdfs_web_port,
            'path_to_target_file': '{{ task_instance.xcom_pull(key="path_to_target_file") }}',
        },
    )

    remove_file_task = BashOperator(
        task_id=f'remove_file_from_YandexCloud',
        bash_command='rm -f "$path_to_source_file";',
        env={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
        },
    )

    send_kafka_user_message_task = PythonOperator(
        task_id='send_kafka_user_message',
        python_callable=send_kafka_user_message,
        op_kwargs={
            'kafka_host': kafka_host,
            'kafka_port': kafka_port,
            'kafka_topic_user_messages': kafka_topic_user_messages,
        },
        provide_context=True,
    )

    trigger_dag = TriggerDagRunOperator(
        task_id=f'trigger-Check_validity_source_file',
        trigger_dag_id='Check_validity_source_file',
        conf={
            'path_to_file': '{{ task_instance.xcom_pull(key="path_to_target_file") }}',
        },
    )

    waiting_for_file >> get_file_directories_task >> prepare_save_path_task >> save_file_to_hdfs_task >> remove_file_task >> send_kafka_user_message_task >> trigger_dag
