from typing import Any

import redis
from django.conf import settings
from redis.typing import KeyT

pool = redis.ConnectionPool(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB,
)


class CommonRedis(redis.Redis):
    """Redis client."""

    def get(self, name: KeyT, default: Any = None) -> KeyT | Any:  # noqa: ANN401
        """Get value from redis."""
        value = super().get(name)
        return value or default


redis_client = CommonRedis(connection_pool=pool)
