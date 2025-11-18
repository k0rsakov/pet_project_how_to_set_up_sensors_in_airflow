import logging

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


# Конфигурация DAG
OWNER = "i.korsakov"
DAG_ID = "dm_dag_with_sensor_on_both_ods_dag"

LONG_DESCRIPTION = """
# LONG DESCRIPTION

"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Описание возможных ключей для default_args
# https://github.com/apache/airflow/blob/343d38af380afad2b202838317a47a7b1687f14f/airflow/example_dags/tutorial.py#L39
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2025, month=1, day=1, tz="UTC"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "depends_on_past": True,
}


def load_dm_layer() -> None:
    """
    Пустышка.

    @return: Ничего не возвращает.
    """

    logging.info("DM layer loaded success ✅.")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["dm"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    sensor_ods_dag_users_to_dwh_pg = ExternalTaskSensor(
        task_id="sensor_ods_dag_users_to_dwh_pg",
        external_dag_id="ods_dag_users_to_dwh_pg",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    sensor_ods_dag_without_catchup = ExternalTaskSensor(
        task_id="sensor_ods_dag_without_catchup",
        external_dag_id="ods_dag_without_catchup",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    load_ods_layer = PythonOperator(
        task_id="load_ods_layer",
        python_callable=load_dm_layer,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_ods_dag_users_to_dwh_pg >> sensor_ods_dag_without_catchup >> load_ods_layer >> end
