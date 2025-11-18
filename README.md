# Как настроить Sensors в Airflow
https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0


https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0

## О видео


## О проекте

### Виртуальное окружение

Настройка виртуального окружения:

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

### Настройка Airflow через Docker

Для запуска контейнера с Airflow, выполните команду:

```bash
docker-compose up -d
```

Веб-сервер Airflow запустится на хосте http://localhost:8080/, если не будет работать данный хост, то необходимо перейти
по хосту http://0.0.0.0:8080/.
