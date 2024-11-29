from collections.abc import Generator

import pytest
from clickhouse_connect.driver import Client
from django.conf import settings

from core.redis_client import redis_client


@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> Generator:
    f_ch_client.query(f'TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}')
    yield


@pytest.fixture(autouse=True)
def f_clean_up_redis() -> None:
    redis_client.flushall()
