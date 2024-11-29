from types import TracebackType

from django.utils import timezone

from core.base_model import Model
from core.event_log_client import EventLogClient


class SafeSendEventLogs:

    def __init__(self, client: EventLogClient) -> None:
        self._client = client
        self._date_time = timezone.now()
        self._logs = None
        self._status = False

    def __enter__(self) -> 'SafeSendEventLogs':
        """Enter."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit."""
        if exc_type is not None:
            self.rollback()

    def send(self, logs: list[Model]) -> None:
        """Send event logs."""
        self._client.insert(logs, self._date_time)
        self._logs = logs
        self._status = True

    def rollback(self) -> None:
        """Rollback."""
        if not self._status:
            return
        self._client.remove(self._logs, self._date_time)
