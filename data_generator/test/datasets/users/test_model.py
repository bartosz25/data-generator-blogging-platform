import datetime

from assertpy import assert_that

from data_generator.datasets.users.model import RegisteredUser


def should_return_user_as_json():
    user = RegisteredUser(id='id 1', login='abc', email='abc@example.com',
                          registered_datetime=datetime.datetime(year=2023, month=5, day=20, hour=10, minute=40,
                                                                second=0),
                          first_connection_datetime=datetime.datetime(year=2023, month=5, day=20, hour=10, minute=40,
                                                                      second=0),
                          last_connection_datetime=None)

    user_dict = user.as_dict()

    assert_that(user_dict).is_equal_to({'email': 'abc@example.com',
                                        'first_connection_datetime': datetime.datetime(2023, 5, 20, 10, 40),
                                        'id': 'id 1',
                                        'last_connection_datetime': None, 'login': 'abc',
                                        'registered_datetime': datetime.datetime(2023, 5, 20, 10, 40)})


def should_return_partition_key_for_the_user():
    user = RegisteredUser(id='id 1', login='abc', email='abc@example.com',
                          registered_datetime=datetime.datetime(year=2023, month=5, day=20, hour=10, minute=40,
                                                                second=0),
                          first_connection_datetime=datetime.datetime(year=2023, month=5, day=20, hour=10, minute=40,
                                                                      second=0),
                          last_connection_datetime=None)

    partition_key = user.partition_key()

    assert_that(partition_key).is_equal_to('abc')
