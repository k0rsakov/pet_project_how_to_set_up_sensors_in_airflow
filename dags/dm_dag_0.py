import logging
import time

import pendulum

from airflow import DAG

# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "ods_dag_0"

LONG_DESCRIPTION = """
# LONG DESCRIPTION

"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2023, month=1, day=1, tz="UTC"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def load_dm_layer(**context) -> None:
    """
    Печатает контекст DAG.

    @param context: Контекст DAG.
    @return: Ничего не возвращает.
    """
    time.sleep(0)
    logging.info("DM layer loaded success ✅.")

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["ods"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    load_ods_layer = PythonOperator(
        task_id="load_ods_layer",
        python_callable=load_dm_layer,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> load_ods_layer >> end