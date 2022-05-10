from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from kafka import KafkaProducer

import os
import datetime

hdfs_host = Variable.get('HDFS_host')
hdfs_port = Variable.get('HDFS_port')
hdfs_web_port = Variable.get('HDFS_web_port')

kafka_host = Variable.get('Kafka_host')
kafka_port = Variable.get('Kafka_port')
kafka_topic_user_messages = Variable.get('Kafka_topic_user_messages')
kafka_topic_admin_messages = Variable.get('Kafka_topic_admin_messages')

# Подключение внешних библиотек
plugins = FSHook(conn_id='fs_plugins').get_path()


def send_kafka_user_message(**kwargs):

    ti = kwargs.get('ti')
    kafka_host = kwargs.get('kafka_host')
    kafka_port = kwargs.get('kafka_port')
    kafka_topic_user_messages = kwargs.get('kafka_topic_user_messages')
    source_file = kwargs.get('source_file')
    target_file = kwargs.get('target_file')
    file_resave_status = kwargs.get('file_resave_status')

    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}')

    if file_resave_status:
        message = f'Файл «{source_file}» был обработан и пересохранен в бинарном формате parquet в файл «{target_file}», перезаписав существующий файл!'
    else:
        message = f'Файл «{source_file}» был обработан и пересохранен в бинарном формате parquet в файл «{target_file}»!'

    producer.send(topic=kafka_topic_user_messages, value=message.encode('UTF-8'))


with DAG(
    dag_id = "Resave_HDFS_csv_to_parquet",
    start_date = datetime.datetime(2022,4,9,0,30,0),
    schedule_interval = None,
    catchup = False,
) as dag:

    resave_csv_to_hdfs_task = SparkSubmitOperator(
        task_id='resave_csv_to_hdfs',
        conn_id='spark_default',
        application=os.path.join(plugins, 'SparkJob-Resave_HDFS_csv_to_parquet.py'),
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        name='resave_csv_to_hdfs',
        verbose=False,
        driver_memory='1g',
        application_args=[
            '--path_to_csv_file', 'hdfs://%(hdfs_host)s:%(hdfs_port)s%(path_to_source_file)s' %{
                'hdfs_host': hdfs_host,
                'hdfs_port': hdfs_port,
                'path_to_source_file': '{{ dag_run.conf.get("path_to_source_file") }}',
            },
            '--path_to_parquet_file', 'hdfs://%(hdfs_host)s:%(hdfs_port)s%(path_to_target_file)s' %{
                'hdfs_host': hdfs_host,
                'hdfs_port': hdfs_port,
                'path_to_target_file': '{{ dag_run.conf.get("path_to_target_file") }}',
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
            'source_file': '{{ dag_run.conf.get("source_file") }}',
            'target_file': '{{ dag_run.conf.get("target_file") }}',
            'file_resave_status': '{{ dag_run.conf.get("file_resave_status") }}',
        },
        provide_context=True,
    )

    resave_csv_to_hdfs_task >> send_kafka_user_message_task