"""
Extract data from Faker service and write to PostgreSQL.
MinIO storage is used for caching data.
"""

from datetime import datetime
import pytz
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator

from hooks import FakerHook, MINIO_HOOK
from utils import insert_to_person


BUCKET_NAME = "simulative-extract-load"

logger = logging.getLogger(__name__)


def _fetch_from_faker(**context):
    """
    Fetch data about a fake person from the Faker service.
    """
    faker_hook = FakerHook(conn_id="FAKER")

    person_dict = faker_hook.get_person()
    key = person_dict["id"]
    person_json = json.dumps(person_dict)

    if not MINIO_HOOK.check_for_bucket(BUCKET_NAME):
        logger.info(f"Bucket {BUCKET_NAME} not exists, create him.")
        MINIO_HOOK.create_bucket(BUCKET_NAME)

    MINIO_HOOK.load_string(person_json, key, BUCKET_NAME, replace=True)
    logger.info(f"Load record to MinIO: {key=}")

    context["ti"].xcom_push(key="object_name", value=key)


def _write_to_postgres(**context):
    """
    Writes data AS IS to the `person' table of the PostgreSQL database.
    """
    object_name = context["ti"].xcom_pull(key="object_name")

    data_json = MINIO_HOOK.read_key(object_name, BUCKET_NAME)
    logger.info(f"Read record from MinIO: {object_name=}")

    data_dict = json.loads(data_json)

    row = tuple(data_dict.values())
    query = (
        "insert into person values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    )

    rowcount = insert_to_person(query, row)  # type: ignore
    logger.info(f"Insert into table 'person', rowcount={rowcount}")


def _cleanup_minio_bucket():
    """
    Service task, remove all objects from the MinIO bucket
    at the end of the processing cycle.
    """
    objects = MINIO_HOOK.list_keys(BUCKET_NAME)
    MINIO_HOOK.delete_objects(BUCKET_NAME, objects)


with DAG(
    dag_id="extract_load_dag",
    start_date=datetime(2025, 1, 1, tzinfo=pytz.timezone("UTC")),
    schedule="*/02 * * * *",
    catchup=False,
    tags=["simulative"],
) as dag:
    fetch_from_faker = PythonOperator(
        task_id="fetch_from_faker",
        python_callable=_fetch_from_faker,
    )

    write_to_postgres = PythonOperator(
        task_id="write_to_postgres", python_callable=_write_to_postgres
    )

    cleanup_minio_bucket = PythonOperator(
        task_id="cleanup_minio_bucket", python_callable=_cleanup_minio_bucket
    )

    (
        fetch_from_faker  # type: ignore (for Pylance only)
        >> write_to_postgres
        >> cleanup_minio_bucket
    )
