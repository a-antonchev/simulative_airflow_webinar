from datetime import datetime
import logging

import psycopg2

from airflow.exceptions import AirflowException

from hooks import POSTGRES_HOOK

logger = logging.getLogger(__name__)


def hand_exceptions(hook):
    def _decorator(func):
        def wrapper(*args, **kwargs):
            conn = None
            try:
                conn = hook.get_conn()
                with conn:
                    result = func(conn, *args, **kwargs)
                return result
            except psycopg2.Error as e:
                error_msg = f"Database error during execute/commit: {e}"
                logger.error(error_msg)
                raise AirflowException(error_msg) from e
            except Exception as e:
                error_msg = f"An unexpected error occurred: {e}"
                logger.error(error_msg, exc_info=True)
                raise AirflowException(error_msg) from e
            finally:
                if conn:
                    logger.info("Closing postgres connection")
                    conn.close()

        return wrapper

    return _decorator


@hand_exceptions(POSTGRES_HOOK)
def insert_to_person(conn, query, row):
    with conn.cursor() as curs:
        curs.execute(query, row)
        rowcount = curs.rowcount

    return rowcount


@hand_exceptions(POSTGRES_HOOK)
def get_min_updated_at(conn, query):
    with conn.cursor() as curs:
        curs.execute(query)
        min_updated_at = curs.fetchone()[0]

    return min_updated_at


@hand_exceptions(POSTGRES_HOOK)
def get_incremental_data(conn, query, last_processed_time, data_interval_end):
    with conn.cursor() as curs:
        curs.execute(query, (last_processed_time, data_interval_end))
        data = curs.fetchall()
        row_count = curs.rowcount

    return data, row_count


def get_last_processed_time(hook, bucket_name):
    last_processed_time_str = hook.read_key("last_processed_time", bucket_name)
    last_processed_time = datetime.fromisoformat(last_processed_time_str)
    return last_processed_time
