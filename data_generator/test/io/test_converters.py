import datetime

from assertpy import assert_that

from data_generator.io.converters import json_converter


def should_covert_datetime_into_string():
    converted = json_converter(datetime.datetime(year=2023, month=6, day=20, hour=4, minute=30, second=59))

    assert_that(converted).is_equal_to('2023-06-20T04:30:59')


def should_not_covert_int_into_string():
    converted = json_converter(1)

    assert_that(converted).is_equal_to(1)
