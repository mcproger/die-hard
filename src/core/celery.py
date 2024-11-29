import os

import structlog
from celery import Celery, signals
from celery.schedules import crontab

__all__ = ('app', )

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

app = Celery('core')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.beat_schedule = {
    'sync-all-data-at-midnight': {
        'task': 'users.tasks.clickhouse_import',
        'schedule': crontab(minute=0, hour=0),
        'kwargs': {
            'force': True,
        },
    },
    'sync-data-every-3-hour': {
        'task': 'users.tasks.clickhouse_import',
        'schedule': crontab(minute=0, hour='3,6,9,12,15,18,21'),
    },
}

app.autodiscover_tasks()


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    """On task prerun."""
    structlog.contextvars.bind_contextvars(task_id=task_id, task_name=task.name)
