import structlog
from celery.utils.log import get_task_logger
from clickhouse_connect.driver.exceptions import DatabaseError as ClickHouseDatabaseError
from django.conf import settings
from django.db import DatabaseError as PostgresqlDatabaseError

from core.celery import app
from core.utils import logger_decorator

from .use_cases.sync_clickhouse import SyncClickHouse, SyncClickHouseRequest

logger = structlog.wrap_logger(get_task_logger(__name__))


@app.task(
    autoretry_for=(ClickHouseDatabaseError, PostgresqlDatabaseError),
    retry_kwargs={
        'max_retries': settings.CLICKHOUSE_IMPORT_MAX_RETRIES,
        'default_retry_delay': settings.CELERY_DEFAULT_RETRY_DELAY,
    })
@logger_decorator
def clickhouse_import(
    max_chunks_send: int = settings.CLICKHOUSE_IMPORT_MAX_CHUNKS_SEND,
    chunk_size: int = settings.CLICKHOUSE_IMPORT_CHUNK_SIZE,
    force: bool = False,
) -> None:
    """Run clickhouse import."""
    logger.info('running clickhouse import task')
    for index in range(max_chunks_send):
        request = SyncClickHouseRequest(chunk_size=chunk_size, force=force)
        response = SyncClickHouse().execute(request)
        logger.info(f'import index {index} successful import {response.send_count} users')
        if response.send_count == 0:
            break
