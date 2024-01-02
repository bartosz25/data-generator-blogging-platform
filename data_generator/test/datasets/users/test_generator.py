from assertpy import assert_that

from data_generator.datasets.users.generator import RegisteredUserEntityGenerator


def should_generate_user_only_once():
    users_generator = RegisteredUserEntityGenerator()

    user_1 = users_generator.generate_row(0)
    user_2 = users_generator.generate_row(0)

    assert_that(user_1).is_equal_to(user_2)


def should_generate_different_users_for_different_indexes():
    users_generator = RegisteredUserEntityGenerator()

    user_1 = users_generator.generate_row(0)
    user_2 = users_generator.generate_row(1)

    assert_that(user_1).is_not_equal_to(user_2)


def should_generate_complete_user():
    users_generator = RegisteredUserEntityGenerator()

    has_first_connection_datetime = False
    has_last_connection_datetime = False
    # last connection and first connection are randomly generated; hence run multiple times to find them
    for i in range(0, 100):
        user = users_generator.generate_row(i)

        assert_that(user.id).is_not_empty()
        assert_that(user.login).is_not_empty()
        assert_that(user.email).is_not_empty()
        assert_that(user.registered_datetime).is_not_none()
        if user.first_connection_datetime:
            has_first_connection_datetime = True
        if user.last_connection_datetime:
            has_last_connection_datetime = True
    assert_that(has_first_connection_datetime).is_true()
    assert_that(has_last_connection_datetime).is_true()
