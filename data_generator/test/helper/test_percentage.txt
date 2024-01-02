from assertpy import assert_that

from data_generator.helper.percentage import calculate_value


def should_calculate_correct_percentage_for_25():
    percentage = calculate_value(200, 25)

    assert_that(percentage).is_equal_to(50)


def should_calculate_correct_percentage_distribution():
    all_numbers = 200
    percentage_25 = calculate_value(all_numbers, 25)
    percentage_44 = calculate_value(all_numbers, 44)
    percentage_31 = calculate_value(all_numbers, 31)

    assert_that(percentage_25 + percentage_31 + percentage_44).is_equal_to(all_numbers)
    assert_that(percentage_25).is_equal_to(50)
    assert_that(percentage_44).is_equal_to(88)
    assert_that(percentage_31).is_equal_to(62)
