FROM postgres:15-alpine

# Копирование скриптов инициализации
COPY ./init /docker-entrypoint-initdb.d/

# Установка postgresql-contrib без временных зависимостей
RUN apk update && \
    apk add --no-cache postgresql-contrib