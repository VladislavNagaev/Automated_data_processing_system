# Образ на основе которого будет создан контейнер
FROM apache/airflow:2.2.5-python3.8

# Выбор root пользователя
USER root

WORKDIR /

# Установка пакетов
RUN apt -y update && \
    # apt upgrade &&\
    echo Y | apt install wget && \
    # Установка C compiler (GCC)
    echo Y | apt install build-essential && \
    apt install manpages-dev && \
    # Установка gssapi
    echo Y | apt install libkrb5-dev && \
    # Install OpenJDK-11
    apt install -y openjdk-11-jdk && \
    apt install -y ant && \
    # Install Spark
    wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
    tar -xf spark-3.2.0-bin-hadoop3.2.tgz && \
    rm spark-3.2.0-bin-hadoop3.2.tgz && \
    # Создание ссылки на spark
    ln -s spark-3.2.0-bin-hadoop3.2/ spark && \
    # libsasl
    apt install libsasl2-dev

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
# Set SPARK_HOME
ENV SPARK_HOME /spark

# Выбор пользователя по-умолчанию
USER airflow

# Установка зависимостей
COPY requirements.txt .
RUN /usr/local/bin/python -m pip install --upgrade pip && \
    pip install --no-cache-dir --requirement requirements.txt