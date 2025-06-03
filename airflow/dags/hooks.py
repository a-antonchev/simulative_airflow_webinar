"""
Custom hooks for interacting with external services.
"""

import requests

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException


MINIO_HOOK = S3Hook(aws_conn_id="MINIO")
POSTGRES_HOOK = PostgresHook(postgres_conn_id="POSTGRES")


class FakerHook(BaseHook):
    """
    A hook for interacting with the Faker service.

    The hook is initialized with a connection ID and an optional timeout.

    The hook provides a single method, get_person, which returns a dictionary
    containing a random person's data.
    """

    def __init__(self, conn_id: str, timeout: int = 10):
        """
        Initialize the hook.

        Args:
            conn_id: The connection ID to use.
            timeout: The timeout to use for requests, in seconds.
        """
        super().__init__()
        self._conn_id = conn_id
        self._session = None
        self._url = None
        self.timeout = timeout

    def _init_connection(self):
        """
        Initialize the connection.

        This method is called automatically by get_person. It initializes the
        connection by getting the Connection object from the database and
        extracting the URL from it.
        """
        if self._session is None or self._url is None:
            conn_config = self.get_connection(self._conn_id)  # -> Connection
            conn_type = conn_config.conn_type
            host = conn_config.host
            port = conn_config.port

        if not all((conn_type, host, port)):
            err_msg = (
                f"Connection {self._conn_id} is misconfigured."
                f"{conn_type=}, {host=}, {port=}"
            )
            self.log.error(err_msg)
            raise AirflowException(err_msg)

        self._url = f"{conn_type}://{host}:{port}/person/"
        self._session = requests.Session()

    def get_person(self) -> dict:
        """
        Get a random person's data.

        This method returns a dictionary containing a random person's data.

        Raises:
            AirflowException: If there is an error communicating with the
                Faker service.
        """
        try:
            self._init_connection()
        except AirflowException as e:
            self.log.info(
                f"Failed to initialize connection for FakerHook: {e}"
            )
            raise

        self.log.info(f"Requesting person data from {self._url}")
        try:
            response = self._session.get(self._url, timeout=self.timeout)
            response_dict = response.json()
            self.log.info(
                f"Successfully received and parsed person data from {self._url}"
            )
            return response_dict
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout error while requesting {self._url}: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error {e.response.status_code} for {self._url}."
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error while requesting {self._url}: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        except requests.exceptions.RequestException as e:
            error_msg = (
                f"An unexpected requests error occurred for {self._url}: {e}"
            )
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
        except Exception as e:
            error_msg = f"An unexpected error occurred in get_person for {self._url}: {e}"
            self.log.error(error_msg)
            raise AirflowException(error_msg) from e
