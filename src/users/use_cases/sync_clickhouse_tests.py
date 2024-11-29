from collections.abc import Callable
from unittest import mock
from unittest.mock import _patch

import pytest
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DatabaseError as ClickHouseError
from django.db.utils import DatabaseError as PostgreSQLError
from faker import Faker

from users.models import ClickHouseSyncLogs, User

from .sync_clickhouse import SyncClickHouse, SyncClickHouseRequest

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def user_factory(faker: Faker) -> Callable:
    """User factory."""

    def _create_item(**kwargs: dict) -> User:
        return User.objects.create(
            email=faker.unique.email(),
            first_name=faker.word(),
            last_name=faker.word(),
            **kwargs)
    return _create_item


@pytest.fixture()
def clickhouse_logs_factory(user_factory: Callable) -> Callable:
    """Clickhouse log factory."""

    def _create_item(user_id: int | None = None, **kwargs: dict) -> ClickHouseSyncLogs:
        if not user_id:
            user = user_factory()
            user_id = user.id
        params = dict(
            user_id=user_id,
            **kwargs)
        return ClickHouseSyncLogs.objects.create(**params)

    return _create_item


@pytest.fixture()
def raise_mock_context_manager() -> Callable:
    """Raise mock"""

    def _wrapper(path: str, error: Exception) -> _patch:
        def _raise_mock(*args: tuple, **kwargs: dict) -> None:  # noqa: ARG001
            raise error()

        return mock.patch(path, side_effect=_raise_mock)
    return _wrapper


@pytest.fixture()
def f_use_case() -> SyncClickHouse:
    return SyncClickHouse()


@pytest.mark.parametrize(
    ('users_count', 'chunk_size', 'expected_logs_count', 'force'),
    [
        (10, 5, 5, False),
        (5, 10, 0, False),
        (5, 10, 5, True),
    ],
)
def test_sync_data_successful(
    user_factory: Callable,
    f_use_case: SyncClickHouse,
    f_ch_client: Client,
    users_count: int,
    chunk_size: int,
    expected_logs_count: int,
    force: bool,
) -> None:
    """Check sync data successful."""
    [user_factory() for _ in range(users_count)]
    request = SyncClickHouseRequest(
        chunk_size=chunk_size,
        force=force)
    f_use_case.execute(request)
    log = f_ch_client.query("SELECT * FROM default.event_log WHERE event_type = 'user_created'")
    assert expected_logs_count == ClickHouseSyncLogs.objects.count()
    assert len(log.result_rows) == expected_logs_count


def test_rollback_with_postgresql_error(
    user_factory: Callable,
    f_use_case: SyncClickHouse,
    f_ch_client: Client,
    raise_mock_context_manager: mock.MagicMock,
) -> None:
    """Check rollback with database error."""
    chunk_size = 5
    users_count = 10
    expected_logs_count = 0
    [user_factory() for _ in range(users_count)]
    request = SyncClickHouseRequest(
        chunk_size=chunk_size)
    with raise_mock_context_manager(
        'users.use_cases.sync_clickhouse.SyncClickHouse._create_sync_logs',
        PostgreSQLError,
    ):
        with pytest.raises(PostgreSQLError):
            f_use_case.execute(request)
    query = ("SELECT * "
             "FROM default.event_log "
             "WHERE event_type = 'user_created' and sign = -1")
    log = f_ch_client.query(query)
    assert ClickHouseSyncLogs.objects.count() == expected_logs_count
    assert len(log.result_rows) == expected_logs_count


def test_rollback_with_clickhouse_error(
    user_factory: Callable,
    f_use_case: SyncClickHouse,
    f_ch_client: Client,
    raise_mock_context_manager: mock.MagicMock,
) -> None:
    """Check rollback with clickhouse error."""
    chunk_size = 5
    users_count = 10
    expected_logs_count = 0
    [user_factory() for _ in range(users_count)]
    request = SyncClickHouseRequest(
        chunk_size=chunk_size)
    with raise_mock_context_manager(
        'core.event_log_client.EventLogClient.insert',
        ClickHouseError,
    ):
        with pytest.raises(ClickHouseError):
            f_use_case.execute(request)
    query = ("SELECT * "
             "FROM default.event_log "
             "WHERE event_type = 'user_created' and sign = -1")
    log = f_ch_client.query(query)
    assert ClickHouseSyncLogs.objects.count() == expected_logs_count
    assert len(log.result_rows) == expected_logs_count


def test_get_only_unsynchronized_users(
    f_use_case: SyncClickHouse,
    user_factory: Callable,
    clickhouse_logs_factory: Callable,
) -> None:
    """Check get only unsynchronized users."""
    sync_user = user_factory()
    clickhouse_logs_factory(user_id=sync_user.id)
    unsynchronized_user = user_factory()
    result = f_use_case._get_unsynchronized_users(chunk_size=20)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0].id == unsynchronized_user.id
