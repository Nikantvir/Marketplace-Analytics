FROM apache/airflow:2.7.1

USER root

# Установка необходимых системных зависимостей
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Установка Python зависимостей для Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Создание необходимых директорий
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

WORKDIR /opt/airflow