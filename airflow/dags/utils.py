"""
Utils for interacting with Postgres and S3.
"""

from datetime import datetime
import logging

import psycopg2

from airflow.exceptions import AirflowException

from hooks import POSTGRES_HOOK

logger = logging.getLogger(__name__)


def hand_exceptions(hook):
    """
    Decorator for handling exceptions when interacting with Postgres.

    :param hook: The Postgres hook to use.
    :return: The decorated function.
    """

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
    """
    Insert a row into the person table.

    :param conn: The Postgres connection to use.
    :param query: The SQL query to execute.
    :param row: The row to insert.
    :return: The number of rows affected.
    """
    with conn.cursor() as curs:
        curs.execute(query, row)
        rowcount = curs.rowcount

    return rowcount


@hand_exceptions(POSTGRES_HOOK)
def get_min_updated_at(conn, query):
    """
    Get the minimum updated_at time from the person table.

    :param conn: The Postgres connection to use.
    :param query: The SQL query to execute.
    :return: The minimum updated_at time.
    """
    with conn.cursor() as curs:
        curs.execute(query)
        min_updated_at = curs.fetchone()[0]

    return min_updated_at


@hand_exceptions(POSTGRES_HOOK)
def get_incremental_data(conn, query, last_processed_time, data_interval_end):
    """
    Get the incremental data from the person table.

    :param conn: The Postgres connection to use.
    :param query: The SQL query to execute.
    :param last_processed_time: The last time data was processed.
    :param data_interval_end: The end of the current data interval.
    :return: The incremental data and the number of rows affected.
    """
    with conn.cursor() as curs:
        curs.execute(query, (last_processed_time, data_interval_end))
        data = curs.fetchall()
        row_count = curs.rowcount

    return data, row_count


def get_last_processed_time(hook, bucket_name):
    """
    Get the last processed time from S3.

    :param hook: The S3 hook to use.
    :param bucket_name: The name of the S3 bucket to use.
    :return: The last processed time.
    """
    last_processed_time_str = hook.read_key("last_processed_time", bucket_name)
    last_processed_time = datetime.fromisoformat(last_processed_time_str)
    return last_processed_time
