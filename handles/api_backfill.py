import logging

import pendulum
import requests

BASE_URL = "http://0.0.0.0:8080/api/v1"
DAG_ID = "ods_dag_without_catchup"
AUTH = ("airflow", "airflow")  # подставь свои логин/пароль, если другие

START_DATE = pendulum.datetime(2025, 1, 1, 10, 0, tz="UTC")
END_DATE = pendulum.datetime(2025, 11, 17, 10, 0, tz="UTC")


def create_dag_run(logical_date: pendulum.DateTime) -> str:
    """
    Создаёт DAGRun на указанную дату.
    Возвращает dag_run_id.

    Важно: НЕ использовать префикс backfill__, он зарезервирован Airflow.
    """
    dag_run_id = f"manual_fill__{logical_date.to_iso8601_string()}"
    payload = {
        "dag_run_id": dag_run_id,
        "logical_date": logical_date.to_iso8601_string(),
        "conf": {},
    }

    url = f"{BASE_URL}/dags/{DAG_ID}/dagRuns"
    logging.info("POST %s", url)
    logging.info("REQUEST JSON: %s", payload)

    resp = requests.post(url, auth=AUTH, json=payload, timeout=10)
    logging.info("STATUS: %s", resp.status_code)
    logging.info("RESPONSE: %s", resp.text.strip())

    # 409 — такой dag_run_id уже есть, считаем нормальным и идём дальше
    if resp.status_code == 409:
        logging.info("DagRun %s уже существует, пропускаю создание", dag_run_id)
        return dag_run_id

    resp.raise_for_status()
    return dag_run_id


def mark_dag_run_success(dag_run_id: str) -> None:
    """
    Ставит DAGRun в состояние success через PATCH /dagRuns/{dag_run_id}.
    """
    url = f"{BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    payload = {"state": "success"}

    logging.info("PATCH %s", url)
    logging.info("REQUEST JSON: %s", payload)

    resp = requests.patch(url, auth=AUTH, json=payload, timeout=10)
    logging.info("STATUS: %s", resp.status_code)
    logging.info("RESPONSE: %s", resp.text.strip())

    resp.raise_for_status()


def iter_dates(start: pendulum.DateTime, end: pendulum.DateTime):
    """
    Простейший генератор дат: шаг 1 день.
    У нас cron '0 10 * * *', так что этого достаточно.
    """
    current = start
    while current <= end:
        yield current
        current = current.add(days=1)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logging.info("API-бэкфилл для DAG: %s", DAG_ID)
    logging.info("Диапазон: %s — %s", START_DATE, END_DATE)

    for logical_date in iter_dates(START_DATE, END_DATE):
        iso = logical_date.to_iso8601_string()
        logging.info("=== logical_date=%s ===", iso)

        dag_run_id = create_dag_run(logical_date)
        mark_dag_run_success(dag_run_id)

    logging.info("Готово. Проверяй DAGRuns в UI.")


if __name__ == "__main__":
    main()