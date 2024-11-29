# Die Hard

This is a project with a test task for backend developers.

You can find detailed requirements by clicking the links:
- [English version](docs/task_en.md)
- [Russian version](docs/task_ru.md)

Tech stack:
- Python 3.13
- Django 5
- pytest
- Docker & docker-compose
- PostgreSQL
- ClickHouse

## Installation

Put a `.env` file into the `src/core` directory. You can start with a template file:

```
cp src/core/.env.ci src/core/.env
```

Run the containers with
```
make run
```

and then run the installation script with:

```
make install
```

## Tests

`make test`

## Linter

`make lint`

## Approach

- use VersionedCollapsingMergeTree instead MergeTree for the purpose 
effective remove data from ClickHouse on database error
- add table ClickHouseSyncLogs to save synchronized logs
- use Celery to realize periodical task for purpose run background tasks 
(Celery used threads worker, because it's IO bound task)
- add support force synchronized logs to make it immediately if you need