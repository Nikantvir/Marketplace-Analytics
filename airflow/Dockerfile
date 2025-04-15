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

# Создаем необходимые директории и устанавливаем права
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/session \
    && chown -R airflow:root /opt/airflow \
    && chmod -R 775 /opt/airflow

# Переключаемся на пользователя airflow для установки Python пакетов
USER airflow

# Установка Python зависимостей
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir pytz python-dateutil

ENV PYTHONPATH="/opt/airflow"
WORKDIR /opt/airflow