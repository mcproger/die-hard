from collections.abc import Callable
from functools import wraps
from typing import Any

import sentry_sdk
import structlog


def logger_decorator(func: Callable) -> Callable:

    @wraps(func)
    def _wrapper(*args: tuple, **kwargs: dict) -> Any:  # noqa: ANN401
        try:
            with structlog.contextvars.bound_contextvars(
                **{'args': args, 'kwargs': kwargs},
            ):
                return func(*args, **kwargs)
        except Exception as exp:
            sentry_sdk.capture_message(repr(exp))
            raise exp
    return _wrapper
