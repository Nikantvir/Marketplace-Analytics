FROM postgres:15-alpine

# Копирование скриптов инициализации
COPY ./init /docker-entrypoint-initdb.d/

# Установка базовых расширений
RUN apk add --no-cache --virtual .build-deps \
    build-base \
    postgresql-dev \
    && apk add --no-cache \
    postgresql-contrib \
    && apk del .build-deps