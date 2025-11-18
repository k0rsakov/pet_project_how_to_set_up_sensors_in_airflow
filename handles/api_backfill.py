import logging
from typing import List

import pendulum
import requests

# === НАСТРОЙКИ ===
BASE_URL = "http://0.0.0.0:8080/api/v1"
DAG_ID = "ods_dag_without_catchup"

# Если включена Basic Auth в Airflow
AUTH = ("airflow", "airflow")  # подставь свои логин/пароль при необходимости

# Диапазон дат, для которых создаём DAGRun'ы
START_DATE = pendulum.datetime(2025, 1, 1, 10, 0, tz="UTC")
END_DATE = pendulum.datetime(2025, 11, 17, 10, 0, tz="UTC")


def create_dag_run(logical_date: pendulum.DateTime) -> str:
    """
    Создаёт DAGRun для DAG_ID на указанную logical_date.
    Возвращает dag_run_id.

    ВАЖНО: не использовать префикс 'backfill__', он зарезервирован Airflow.
    """
    dag_run_id = f"manual_fill__{logical_date.to_iso8601_string()}"
    payload = {
        "dag_run_id": dag_run_id,
        "logical_date": logical_date.to_iso8601_string(),
        "conf": {},
    }
    url = f"{BASE_URL}/dags/{DAG_ID}/dagRuns"

    logging.info("Создаю DAGRun: %s, logical_date=%s", dag_run_id, payload["logical_date"])
    resp = requests.post(url, auth=AUTH, json=payload, timeout=10)

    logging.info("POST %s -> %s %s", url, resp.status_code, resp.text.strip())

    # 409 — такой dag_run_id уже существует, считаем это ок
    if resp.status_code == 409:
        logging.info("DAGRun %s уже существует, пропускаю создание", dag_run_id)
        return dag_run_id

    resp.raise_for_status()
    return dag_run_id


def list_task_instances(dag_run_id: str) -> List[dict]:
    """
    Возвращает список TaskInstance для указанного dag_run_id.
    """
    url = f"{BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances"
    resp = requests.get(url, auth=AUTH, timeout=10)
    logging.info("GET %s -> %s", url, resp.status_code)
    resp.raise_for_status()
    data = resp.json()
    return data.get("task_instances", [])


def mark_task_success(dag_run_id: str, task_id: str) -> None:
    """
    Помечает указанный TaskInstance как success.
    """
    url = f"{BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
    payload = {"state": "success"}
    resp = requests.patch(url, auth=AUTH, json=payload, timeout=10)
    logging.info(
        "PATCH %s (task_id=%s) -> %s %s",
        url,
        task_id,
        resp.status_code,
        resp.text.strip(),
    )
    resp.raise_for_status()


def mark_all_tasks_success(dag_run_id: str) -> None:
    """
    Получает все таски в DAGRun и помечает их success.
    """
    tis = list_task_instances(dag_run_id)
    if not tis:
        logging.warning("У DAGRun %s нет task instances (пока?).", dag_run_id)
        return

    for ti in tis:
        task_id = ti["task_id"]
        state = ti.get("state")
        # Если уже success — можно не трогать
        if state == "success":
            logging.info("Task %s уже success, пропускаю", task_id)
            continue
        mark_task_success(dag_run_id, task_id)


def iter_logical_dates(
    start: pendulum.DateTime,
    end: pendulum.DateTime,
    cron_expr: str = "0 10 * * *",
):
    """
    Генератор дат по cron-расписанию между start и end включительно.

    Для простоты здесь шаг = 1 день, т.к. у нас "0 10 * * *".
    Если хочешь строго по cron, можно использовать pendulum + croniter.
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

    logging.info("Запускаю API-бэкфилл для DAG: %s", DAG_ID)
    logging.info("Диапазон: %s — %s", START_DATE, END_DATE)

    for logical_date in iter_logical_dates(START_DATE, END_DATE):
        logging.info("=== Обработка logical_date=%s ===", logical_date.to_iso8601_string())
        dag_run_id = create_dag_run(logical_date)
        mark_all_tasks_success(dag_run_id)

    logging.info("Готово. Проверяй DAGRuns в UI для DAG %s.", DAG_ID)


if __name__ == "__main__":
    main()