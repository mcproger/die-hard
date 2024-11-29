import datetime
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import clickhouse_connect
import structlog
from clickhouse_connect.driver.exceptions import DatabaseError
from django.conf import settings
from django.utils import timezone

from core.base_model import Model

logger = structlog.get_logger(__name__)

EVENT_LOG_COLUMNS = [
    'event_type',
    'event_date_time',
    'environment',
    'event_context',
    'sign',
]

CLICKHOUSE_SIGN_INSERT_DATA = 1
CLICKHOUSE_SIGN_REMOVE_DATA = -1


class EventLogClient:
    def __init__(self, client: clickhouse_connect.driver.Client) -> None:
        self._client = client

    @classmethod
    @contextmanager
    def init(cls) -> Generator['EventLogClient']:
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            query_retries=2,
            connect_timeout=30,
            send_receive_timeout=10,
        )
        try:
            yield cls(client)
        finally:
            client.close()

    def insert(
        self,
        data: list[Model],
        date_time: datetime.datetime | None = None,
    ) -> datetime.datetime:
        """Insert data."""
        if date_time is None:
            date_time = timezone.now()
        self._send_data(data, date_time)
        return date_time

    def remove(
        self,
        data: list[Model],
        event_date_time: datetime.datetime,
    ) -> None:
        """Remove data."""
        self._send_data(data, event_date_time, is_remove=True)

    def _send_data(
        self,
        data: list[Model],
        event_date_time: datetime.datetime,
        is_remove: bool = False,
    ) -> None:
        try:
            self._client.insert(
                data=self._convert_data(
                    data,
                    event_date_time,
                    is_remove),
                column_names=EVENT_LOG_COLUMNS,
                database=settings.CLICKHOUSE_SCHEMA,
                table=settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME,
            )
        except DatabaseError as e:
            logger.error('unable to insert data to clickhouse', error=str(e))
            raise e

    def query(self, query: str) -> Any:  # noqa: ANN401
        logger.debug('executing clickhouse query', query=query)

        try:
            return self._client.query(query).result_rows
        except DatabaseError as e:
            logger.error('failed to execute clickhouse query', error=str(e))
            return

    def _convert_data(
        self,
        data: list[Model],
        event_date_time: datetime.datetime,
        is_remove: bool,
    ) -> list[tuple[Any]]:
        """Convert data before send."""
        result = []
        for event in data:
            converted_event = (
                self._to_snake_case(event.__class__.__name__),
                event_date_time,
                settings.ENVIRONMENT,
                event.model_dump_json(),
                CLICKHOUSE_SIGN_REMOVE_DATA if is_remove else CLICKHOUSE_SIGN_INSERT_DATA)
            result.append(converted_event)
        return result

    def _to_snake_case(self, event_name: str) -> str:
        result = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', event_name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', result).lower()

