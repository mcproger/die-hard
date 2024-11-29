from typing import Any

import structlog
from django.db import transaction

from core.use_case import UseCase, UseCaseRequest, UseCaseResponse
from users.models import User

logger = structlog.get_logger(__name__)


class CreateUserRequest(UseCaseRequest):
    email: str
    first_name: str = ''
    last_name: str = ''


class CreateUserResponse(UseCaseResponse):
    result: User | None = None
    error: str = ''


class CreateUser(UseCase):
    def _get_context_vars(self, request: UseCaseRequest) -> dict[str, Any]:
        return {
            'email': request.email,
            'first_name': request.first_name,
            'last_name': request.last_name,
        }

    @transaction.atomic()
    def _execute(self, request: CreateUserRequest) -> CreateUserResponse:
        logger.info('creating a new user')
        user, created = User.objects.get_or_create(
            email=request.email,
            defaults={
                'first_name': request.first_name,
                'last_name': request.last_name,
            },
        )
        if not created:
            logger.error('unable to create a new user')
            return CreateUserResponse(error='User with this email already exists')
        logger.info('user has been created')
        return CreateUserResponse(result=user)
