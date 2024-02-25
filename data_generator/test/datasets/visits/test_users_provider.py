import datetime

from assertpy import assert_that

from data_generator.datasets.users.generator import RegisteredUserEntityGenerator
from data_generator.datasets.users.model import RegisteredUser
from data_generator.datasets.visits.users_providers import UserContextWithUserProvider, RandomUserContextProvider


def should_generate_users_randomly():
    users_provider = RandomUserContextProvider()

    context, user_id = users_provider.provide(0, datetime.datetime(year=2024, month=3, day=10, hour=5, minute=55,
                                                                   second=0))

    assert_that(user_id).is_not_empty()
    assert_that(context.login).is_not_empty()
    assert_that(context.ip).is_not_empty()


def should_generate_users_from_registered_users_generator():
    users_entity_generator = RegisteredUserEntityGenerator()
    user: RegisteredUser = users_entity_generator.generate_row(0)
    users_provider = UserContextWithUserProvider(users_entity_generator)

    context, user_id = users_provider.provide(0, datetime.datetime(year=2024, month=3, day=10, hour=5, minute=55,
                                                                   second=0))

    assert_that(user_id).is_equal_to(user.id)
    assert_that(context.login).is_equal_to(user.login)
    assert_that(context.connected_since).is_equal_to(user.last_connection_datetime)
    assert_that(context.ip).is_not_empty()


def should_fallback_to_random_user_generation_if_users_dataset_is_smaller_than_users_provider():
    users_entity_generator = RegisteredUserEntityGenerator()
    users_provider = UserContextWithUserProvider(users_entity_generator)

    # we're using here a greater index than the indexes used for the generation so far
    context, user_id = users_provider.provide(100, datetime.datetime(year=2024, month=3, day=10, hour=5, minute=55,
                                                                     second=0))

    assert_that(user_id).is_not_empty()
    assert_that(context.login).is_not_empty()
    assert_that(context.ip).is_not_empty()
