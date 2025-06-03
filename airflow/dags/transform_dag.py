""" """

from datetime import datetime
import json
from io import StringIO
import logging

import pandas as pd
import clickhouse_driver

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

from hooks import MINIO_HOOK
from utils import (
    get_last_processed_time,
    get_min_updated_at,
    get_incremental_data,
)

BUCKET_NAME = "simulative-transform"

logger = logging.getLogger(__name__)


def _init_last_processed_time():
    if not MINIO_HOOK.check_for_bucket(BUCKET_NAME):
        logger.info(f"Bucket {BUCKET_NAME} not exists, create him.")
        MINIO_HOOK.create_bucket(BUCKET_NAME)

    if not MINIO_HOOK.check_for_key("last_processed_time", BUCKET_NAME):
        logger.info(
            "Get data for the first time, using the minimum of update_at"
        )

        query = """
        select
        min(updated_at)
        from person;
        """

        last_processed_time = get_min_updated_at(query)  # type: ignore

        MINIO_HOOK.load_string(
            last_processed_time.isoformat(),
            "last_processed_time",
            BUCKET_NAME,
            replace=True,
        )
        logger.info(
            f"Load record to MinIO: key=last_processed_time,"
            f" value={last_processed_time.isoformat()}"
        )
    else:
        logger.info(
            "Found lst_processed_time, skip."
        )


def _check_for_new_data(**context):
    # проверка на новые данные с момента последней инкрементальной загрузки

    last_processed_time = get_last_processed_time(MINIO_HOOK, BUCKET_NAME)
    data_interval_end = context["data_interval_end"]

    logger.info(
        f"Check for new data for interval: {last_processed_time} - {data_interval_end}"
    )

    query = """
    select *
    from person
    where updated_at between %s and %s;
    """

    data, *_ = get_incremental_data(
        query, last_processed_time, data_interval_end
    )  # type: ignore

    if data:
        logger.info("Found new data, try to getting incremental data")
        return "get_incremental_data"
    else:
        logger.info("No new data, re-run dag")
        return "re_run_dag"
    # ----


def _get_incremental_data(**context):
    last_processed_time = get_last_processed_time(MINIO_HOOK, BUCKET_NAME)
    data_interval_end = context["data_interval_end"]

    logger.info(
        f"Getting incremental data for interval: {last_processed_time} - {data_interval_end}"
    )

    columns = [
        "id",
        "name",
        "age",
        "address",
        "email",
        "phone_number",
        "registration_date",
        "created_at",
        "updated_at",
        "deleted_at",
    ]

    query = """
    select *
    from person
    where updated_at >= %s
    and updated_at < %s;
    """

    incremental_data, row_count = get_incremental_data(
        query, last_processed_time, data_interval_end
    )  # type: ignore

    logger.info(f"Get incremental data, rowcount={row_count}")

    df = pd.DataFrame(incremental_data, columns=columns)

    data_json_out = df.to_json(orient="records", date_format="iso")

    MINIO_HOOK.load_string(
        data_json_out,
        "incremental_data",
        BUCKET_NAME,
        replace=True,
    )
    logger.info("Load record to MinIO: key=incremental_data")

    MINIO_HOOK.load_string(
        data_interval_end.isoformat(),
        "last_processed_time",
        BUCKET_NAME,
        replace=True,
    )
    logger.info(
        f"Load record to MinIO: key=last_processed_time, value={data_interval_end.isoformat()}"
    )


def _split_by_cities(**context):
    data_json_in = MINIO_HOOK.read_key("incremental_data", BUCKET_NAME)

    buffer = StringIO(data_json_in)

    df = pd.read_json(buffer)

    df["city"] = df["address"].map(lambda addr: addr.split(",")[0])

    cities = []

    for city in df["city"].unique():
        cities.append(city)

        data_json_out = df.loc[df["city"] == city].to_json(
            date_format="iso", orient="records"
        )
        MINIO_HOOK.load_string(
            data_json_out,
            city,
            BUCKET_NAME,
            replace=True,
        )
        logger.info(f"Load record to MinIO: key={city}")

    logger.info(f"Total city count={len(cities)}")
    # записать в xcom список обработанных городов
    context["ti"].xcom_push(key="cities", value=cities)


def _group_by_city(**context):
    cities = context["ti"].xcom_pull(key="cities")

    cities_groupped = []

    for city in cities:
        data_json_in = MINIO_HOOK.read_key(city, BUCKET_NAME)
        buffer = StringIO(data_json_in)
        df = pd.read_json(buffer)
        data_json_out = (
            df.groupby(by=["city"])
            .agg(cnt=("city", "count"))
            .reset_index()
            .to_json(orient="records")
        )
        object_name = f"{city}_groupped"
        cities_groupped.append(object_name)

        MINIO_HOOK.load_string(
            data_json_out,
            object_name,
            BUCKET_NAME,
            replace=True,
        )
        logger.info(f"Load record to MinIO: key={object_name}")

    logger.info(f"Total city count={len(cities_groupped)}")
    context["ti"].xcom_push(key="cities_groupped", value=cities_groupped)


def _aggregate_data(**context):
    cities_groupped = context["ti"].xcom_pull(key="cities_groupped")

    df = pd.DataFrame()

    for city in cities_groupped:
        data_json_in = MINIO_HOOK.read_key(city, BUCKET_NAME)
        buffer = StringIO(data_json_in)
        df_city = pd.read_json(buffer)
        df = pd.concat([df, df_city], axis=0)

    data_json_out = df.to_json(orient="records")

    MINIO_HOOK.load_string(
        data_json_out,
        "aggregated",
        BUCKET_NAME,
        replace=True,
    )
    logger.info(
        f"Load record to MinIO: key=aggregated, city count={len(cities_groupped)}"
    )


def _write_to_clickhouse():
    ch_conn = BaseHook.get_connection(conn_id="CLICKHOUSE")

    conn = clickhouse_driver.connect(
        host=ch_conn.host,
        port=ch_conn.port,
        database=ch_conn.schema,
        user=ch_conn.login,
        password=ch_conn.password,
    )

    data_json_in = MINIO_HOOK.read_key("aggregated", BUCKET_NAME)
    data_dict = json.loads(data_json_in)

    query = """
    insert into person_count_by_city values
    """

    try:
        with conn:
            with conn.cursor() as curs:
                curs.execute(query, data_dict)
                rowcount = curs.rowcount
        logger.info(
            f"Insert into table 'person_count_by_city values', rowcount={rowcount}"
        )
    except Exception as e:
        logger.error(f"Error execute query: {e}", exc_info=True)
        raise
    finally:
        conn.close()


with DAG(
    dag_id="simulative_advanced_dag_7",
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["simulative"],
) as dag:
    wait_for_data = SqlSensor(
        task_id="wait_for_data",
        conn_id="POSTGRES",
        sql="select * from person;",
        poke_interval=60,
    )

    init_last_processed_time = PythonOperator(
        task_id="init_last_processed_time",
        python_callable=_init_last_processed_time,
    )

    check_for_new_data = BranchPythonOperator(
        task_id="check_for_new_data",
        python_callable=_check_for_new_data,
        provide_context=True,
    )

    get_data = PythonOperator(
        task_id="get_incremental_data", python_callable=_get_incremental_data
    )
    with TaskGroup(group_id="transform_data") as transform_group:
        split_by_cities = PythonOperator(
            task_id="split_by_cities", python_callable=_split_by_cities
        )

        group_by_city = PythonOperator(
            task_id="group_by_city", python_callable=_group_by_city
        )

        aggregate_data = PythonOperator(
            task_id="aggregate_data", python_callable=_aggregate_data
        )

        split_by_cities >> group_by_city >> aggregate_data  # type: ignore

    write_to_clickhouse = PythonOperator(
        task_id="write_to_clickhouse", python_callable=_write_to_clickhouse
    )
    re_run_dag = EmptyOperator(
        task_id="re_run_dag", trigger_rule="none_failed"
    )

    wait_for_data >> init_last_processed_time >> check_for_new_data  # type: ignore
    check_for_new_data >> [get_data, re_run_dag]  # type: ignore
    (
        get_data  # type: ignore
        >> transform_group
        >> write_to_clickhouse
        >> re_run_dag
    )
