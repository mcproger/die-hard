from contextlib import ExitStack
from typing import Any

import structlog
from django.db import transaction

from core.base_model import Model
from core.event_log_client import EventLogClient
from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from users.models import ClickHouseSyncLogs, User
from users.uow import SafeSendEventLogs

logger = structlog.getLogger(__name__)


class UserCreated(Model):
    """User created DTO."""

    email: str
    first_name: str
    last_name: str


class SyncClickHouseRequest(UseCaseRequest):
    """Send data to ClickHouse request."""

    chunk_size: int
    force: bool = False


class SyncClickHouseResponse(UseCaseResponse):
    """Send data to ClickHouse response."""

    send_count: int


class SyncClickHouse(UseCase):

    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:
        return {
            'chunk_size': request.chunk_size,
            'force': request.force,
        }

    @staticmethod
    def _get_unsynchronized_users(chunk_size: int) -> list[User]:
        """Receive unsynchronized logs."""
        queryset = (
            User.objects
            .prefetch_related('clickhouse_sync')
            .filter(clickhouse_sync=None)[:chunk_size])
        return list(queryset)

    @staticmethod
    def _transform_data(instances: list[User]) -> list[UserCreated]:
        """Transform data to send in clickhouse."""
        result = []
        for instance in instances:
            result.append(
                UserCreated(
                    email=instance.email,
                    first_name=instance.first_name,
                    last_name=instance.last_name,
                ))
        return result

    @staticmethod
    def _create_sync_logs(users: list[User]) -> None:
        """Update database info."""
        sync_logs = []
        for user in users:
            sync_logs.append(ClickHouseSyncLogs(user=user))
        ClickHouseSyncLogs.objects.bulk_create(sync_logs)

    def execute(self, request: SyncClickHouseRequest) -> SyncClickHouseResponse:
        """Execute."""
        with ExitStack() as stack:
            stack.enter_context(transaction.atomic())
            stack.enter_context(structlog.contextvars.bound_contextvars(**self._get_context_vars(request)))
            client = stack.enter_context(EventLogClient.init())
            uow = stack.enter_context(SafeSendEventLogs(client))
            return self._execute(request, uow)

    def _execute(
        self,
        request: SyncClickHouseRequest,
        uow: SafeSendEventLogs,
    ) -> SyncClickHouseResponse:
        """Recieve and send chunk data to clickhouse."""
        unsynchronized_users_count = User.objects.filter(clickhouse_sync=None).count()
        if not unsynchronized_users_count:
            logger.info('all users was synchronized')
            return SyncClickHouseResponse(send_count=0)
        if unsynchronized_users_count < request.chunk_size and not request.force:
            logger.info('not enough unsynchronized users')
            return SyncClickHouseResponse(send_count=0)
        users = self._get_unsynchronized_users(request.chunk_size)
        data = self._transform_data(users)
        self._create_sync_logs(users)
        uow.send(data)
        logger.info(f'sync {len(users)} user')
        return SyncClickHouseResponse(send_count=len(data))
