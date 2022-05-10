from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator
from kafka import KafkaConsumer


import datetime

import logging
import requests


telegram_token = Variable.get('Telegram_token')
telegram_chat_id = Variable.get('Telegram_chat_id')

kafka_host = Variable.get('Kafka_host')
kafka_port = Variable.get('Kafka_port')
kafka_topic_user_messages = Variable.get('Kafka_topic_user_messages')
kafka_topic_admin_messages = Variable.get('Kafka_topic_admin_messages')


def send_messeage_to_telegram(token, message, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {'chat_id': chat_id, 'text': message}
    response = requests.post(url, data=params)
    return response


def await_messages(**kwargs):

    kafka_topic_user_messages = kwargs.get('kafka_topic_user_messages')
    kafka_host = kwargs.get('kafka_host')
    kafka_port = kwargs.get('kafka_port')

    telegram_token = kwargs.get('telegram_token')
    telegram_chat_id = kwargs.get('telegram_chat_id')

    consumer = KafkaConsumer(kafka_topic_user_messages, bootstrap_servers=[f'{kafka_host}:{kafka_port}'])

    while True:
        raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
        if len(raw_messages) >= 1:
            for topic_partition, messages in raw_messages.items():
                for message in messages:
                    topic, _, offset, timestamp, _, key, value, _, _, _, _, _ = message
                    logging.info(message)
                    response = send_messeage_to_telegram(token=telegram_token, chat_id=telegram_chat_id, message=value.decode('UTF-8'))
                    logging.info(response)


with DAG(
    dag_id="Send_messages_to_user",
    start_date=datetime.datetime(2022, 4, 9, 0, 30, 0),
    schedule_interval = None,
    catchup=False,
) as dag:


    await_messages_task = PythonOperator(
        task_id='await_messages',
        python_callable=await_messages,
        op_kwargs={
            'telegram_token': telegram_token,
            'telegram_chat_id': telegram_chat_id,
            'kafka_host': kafka_host,
            'kafka_port': kafka_port,
            'kafka_topic_user_messages': kafka_topic_user_messages,
        },
        provide_context=True,
    )

    await_messages_task
