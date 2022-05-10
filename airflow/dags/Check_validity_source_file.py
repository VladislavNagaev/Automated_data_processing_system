from tabnanny import check
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kafka import KafkaProducer

import os
import datetime
import requests
import logging


hdfs_host = Variable.get('HDFS_host')
hdfs_port = Variable.get('HDFS_port')
hdfs_web_port = Variable.get('HDFS_web_port')

kafka_host = Variable.get('Kafka_host')
kafka_port = Variable.get('Kafka_port')
kafka_topic_user_messages = Variable.get('Kafka_topic_user_messages')
kafka_topic_admin_messages = Variable.get('Kafka_topic_admin_messages')

# Подключение внешних библиотек
plugins = FSHook(conn_id='fs_plugins').get_path()

source_working_directory = Variable.get('HDFS_UWCA')
source_data_folder = Variable.get('UWCA_source_csv_folder')
source_data_format = 'csv'

target_working_directory = source_working_directory
target_data_folder = Variable.get('UWCA_source_parquet_folder')
target_data_format = 'parquet'

source_data_path = os.path.join(source_working_directory, source_data_folder)
target_data_path = os.path.join(target_working_directory, target_data_folder)



def get_hdfs_directory_files(hdfs_host, hdfs_web_port, directory):
    """
    Get all file names from directory.
    """

    def __hdfs_request__(url):
        logging.info('Execution of request <LISTSTATUS> to HDFS')
        response = requests.get(url)
        if response.status_code == 200:
            logging.info('Request successful completed with status code', response.status_code)
            json_response = response.json()   
        else:
            logging.info('Request failed with status code', response.status_code)
            json_response = None
        return json_response


    def __parse_webhdfs_resopne__(json_response):
        files_list = json_response.get('FileStatuses').get('FileStatus')
        return files_list
    

    url = f"http://{hdfs_host}:{hdfs_web_port}/webhdfs/v1/{directory[1:]}/?op=LISTSTATUS"

    json_response = __hdfs_request__(url=url)

    if json_response is not None:
        files_list = __parse_webhdfs_resopne__(json_response=json_response)
    else:
        files_list = []

    return files_list


def get_file_directories(**kwargs):

    ti = kwargs.get('ti')
    hdfs_host = kwargs.get('hdfs_host')
    hdfs_web_port = kwargs.get('hdfs_web_port')
    target_data_path = kwargs.get('target_data_path')
    target_data_format = kwargs.get('target_data_format')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    path_to_file = conf.get('path_to_file')

    if path_to_file is None:
        admin_message = 'DAG был запущен, но не были переданы требуемые параметры: <path_to_file>!'
        ti.xcom_push(key='admin_message', value=admin_message)
        raise SystemError('DAG был запущен, но не были переданы требуемые параметры: <path_to_file>!')

    target_files_list = get_hdfs_directory_files(
        hdfs_host=hdfs_host, 
        hdfs_web_port=hdfs_web_port, 
        directory=target_data_path
    )
    target_file_names_list = [''.join(os.path.splitext(file.get('pathSuffix'))[:-1]) for file in target_files_list]

    file = os.path.basename(path_to_file)
    file_name = ''.join(os.path.splitext(file)[:-1])

    if file_name in target_file_names_list:
        file_resave_status = True
    else:
        file_resave_status = False

    target_file = file_name + '.' + target_data_format
    path_to_target_file = os.path.join(target_data_path, target_file)
    
    ti.xcom_push(key='path_to_source_file', value=path_to_file)
    ti.xcom_push(key='path_to_target_file', value=path_to_target_file)

    ti.xcom_push(key='source_file', value=file)
    ti.xcom_push(key='target_file', value=target_file)

    ti.xcom_push(key='file_resave_status', value=file_resave_status)


def send_kafka_admin_message(context):

    ti = context.get('ti')
    admin_message = ti.xcom_pull(key='admin_message')

    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}')
    producer.send(topic=kafka_topic_admin_messages, value=admin_message.encode('UTF-8'))


def send_kafka_user_message(**kwargs):

    ti = kwargs.get('ti')
    source_file = ti.xcom_pull(key='source_file')
    kafka_host = kwargs.get('kafka_host')
    kafka_port = kwargs.get('kafka_port')
    kafka_topic_user_messages = kwargs.get('kafka_topic_user_messages')

    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}')
    message = f'Файл «{source_file}» проверен и отправлен на дальнейшую обработку!'
    producer.send(topic=kafka_topic_user_messages, value=message.encode('UTF-8'))


def check_csv_file_error(context):

    ti = context.get('ti')
    source_file = ti.xcom_pull(key='source_file')
    task_id = context['task'].task_id

    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}')

    if task_id == 'check_csv_file_exist':
        message = f'Файл «{source_file}» не найден в рабочей директории. Дальнейшая обработка прервана!'
    elif task_id == 'check_csv_file_nonempty':
        message = f'Файл «{source_file}» пуст. Дальнейшая обработка прервана!'
    elif task_id == 'check_csv_file_valid':
        message = f'Файл «{source_file}» не валиден: отсуствуют необходимые параметры. Дальнейшая обработка прервана!'

    producer.send(topic=kafka_topic_user_messages, value=message.encode('UTF-8'))


def get_hdfs_file_status(hdfs_host, hdfs_web_port, path_to_file):
    """
    Get status of the file in hdfs.
    """

    def __hdfs_request__(url):
        response = requests.get(url)
        if response.status_code == 200:
            json_response = response.json()   
        else:
            json_response = None
        return json_response


    def __parse_webhdfs_resopne__(json_response):
        files_status = json_response.get('FileStatus')
        return files_status
    

    url = f"http://{hdfs_host}:{hdfs_web_port}/webhdfs/v1/{path_to_file[1:]}?op=GETFILESTATUS"

    json_response = __hdfs_request__(url=url)

    if json_response is not None:
        files_status = __parse_webhdfs_resopne__(json_response=json_response)
    else:
        files_status = None

    return files_status


def check_csv_file_exist(**kwargs):

    ti = kwargs.get('ti')
    source_file = ti.xcom_pull(key='source_file')
    path_to_source_file = ti.xcom_pull(key='path_to_source_file')
    hdfs_host = kwargs.get('hdfs_host')
    hdfs_web_port = kwargs.get('hdfs_web_port')

    files_status = get_hdfs_file_status(
        hdfs_host=hdfs_host, 
        hdfs_web_port=hdfs_web_port, 
        path_to_file=path_to_source_file
    )

    if files_status is None:
        raise ValueError(f'Файл «{source_file}» не найден в рабочей директории!')



with DAG(
    dag_id = "Check_validity_source_file",
    start_date = datetime.datetime(2022,4,9,0,30,0),
    schedule_interval = None,
    catchup = False,
) as dag:

    get_file_directories_task = PythonOperator(
        task_id='get_file_directories',
        python_callable=get_file_directories,
        op_kwargs={
            'hdfs_host': hdfs_host, 
            'hdfs_web_port': hdfs_web_port,
            'source_data_path': source_data_path,
            'target_data_path': target_data_path,
            'target_data_format': target_data_format,
            'kafka_host': kafka_host,
            'kafka_port': kafka_port,
            'kafka_topic_admin_messages': kafka_topic_admin_messages,
        },
        provide_context=True,
        on_failure_callback=send_kafka_admin_message,
    )

    check_csv_file_exist_task = PythonOperator(
        task_id='check_csv_file_exist',
        python_callable=check_csv_file_exist,
        op_kwargs={
            'hdfs_host': hdfs_host, 
            'hdfs_web_port': hdfs_web_port,
        },
        provide_context=True,
        on_failure_callback=check_csv_file_error,
    )

    check_csv_file_nonempty_task = SparkSubmitOperator(
        task_id='check_csv_file_nonempty',
        conn_id='spark_default',
        application=os.path.join(plugins, 'SparkJob-Check_csv_file_nonempty.py'),
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name='check_csv_file_nonempty',
        verbose=False,
        driver_memory='1g',
        on_failure_callback=check_csv_file_error,
        application_args=[
            '--path_to_csv_file', 'hdfs://%(hdfs_host)s:%(hdfs_port)s%(path_to_source_file)s' %{
                'hdfs_host': hdfs_host,
                'hdfs_port': hdfs_port,
                'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            },
        ],
    )

    check_csv_file_valid_task = SparkSubmitOperator(
        task_id='check_csv_file_valid',
        conn_id='spark_default',
        application=os.path.join(plugins, 'SparkJob-Check_csv_file_valid.py'),
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name='check_csv_file_valid',
        verbose=False,
        driver_memory='1g',
        on_failure_callback=check_csv_file_error,
        application_args=[
            '--path_to_csv_file', 'hdfs://%(hdfs_host)s:%(hdfs_port)s%(path_to_source_file)s' %{
                'hdfs_host': hdfs_host,
                'hdfs_port': hdfs_port,
                'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            },
        ],
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
        task_id=f'trigger-Resave_HDFS_csv_to_parquet',
        trigger_dag_id='Resave_HDFS_csv_to_parquet',
        conf={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            'path_to_target_file': '{{ task_instance.xcom_pull(key="path_to_target_file") }}',
            'source_file': '{{ task_instance.xcom_pull(key="source_file") }}',
            'target_file': '{{ task_instance.xcom_pull(key="target_file") }}',
            'file_resave_status': '{{ task_instance.xcom_pull(key="file_resave_status") }}',
        },
    )

    get_file_directories_task >> check_csv_file_exist_task >> check_csv_file_nonempty_task >> check_csv_file_valid_task >> send_kafka_user_message_task >> trigger_dag