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
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir pytz python-dateutil Flask-Session==0.5.0

# Применение патча для Flask-Session
COPY patches/ /opt/airflow/patches/
COPY entrypoint-script.sh /entrypoint-script.sh
RUN chmod +x /entrypoint-script.sh

# Создание необходимых директорий
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/session

ENV PYTHONPATH="/opt/airflow"
WORKDIR /opt/airflow
ENTRYPOINT ["/entrypoint-script.sh"]