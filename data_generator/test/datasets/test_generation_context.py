from assertpy import assert_that

from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.generation_context import DatasetGenerationContext, \
    NotBlockingDataGenerationBlocker


def should_generate_dataset_without_issues():
    context = DatasetGenerationContext(number_of_rows=100, duplicates_percentage=0, late_rows_percentage=0,
                                       missing_fields_percentage=0, unprocessable_rows_percentage=0,
                                       irregular_data_blocker=NotBlockingDataGenerationBlocker(),
                                       entity_generator=DeviceEntityGenerator())

    rows_to_generate = context.get_rows_to_generate_with_maybe_decorators()

    assert_that(rows_to_generate).is_length(100)
    classes = list([type(row).__name__ for row in rows_to_generate])
    expected_classes = ['EmptyRowDecorator']*100
    assert_that(classes).is_in(expected_classes)


def should_generate_dataset_with_issues_only():
    context = DatasetGenerationContext(number_of_rows=100, duplicates_percentage=33, late_rows_percentage=0,
                                       missing_fields_percentage=33, unprocessable_rows_percentage=34,
                                       irregular_data_blocker=NotBlockingDataGenerationBlocker(),
                                       entity_generator=DeviceEntityGenerator())

    rows_to_generate = context.get_rows_to_generate_with_maybe_decorators()

    assert_that(rows_to_generate).is_length(100)
    classes = list([type(row).__name__ for row in rows_to_generate])
    expected_classes = (['DuplicatesRowDecorator']*33 + ['MissingFieldsRowDecorator'] * 33 +
                        ['UnprocessableRecordRowDecorator'] * 34)
    assert_that(classes).is_in(expected_classes)


def should_generate_dataset_with_issues_and_good_data():
    context = DatasetGenerationContext(number_of_rows=100, duplicates_percentage=15,  late_rows_percentage=0,
                                       missing_fields_percentage=15, unprocessable_rows_percentage=20,
                                       irregular_data_blocker=NotBlockingDataGenerationBlocker(),
                                       entity_generator=DeviceEntityGenerator())

    rows_to_generate = context.get_rows_to_generate_with_maybe_decorators()

    assert_that(rows_to_generate).is_length(100)
    classes = list([type(row).__name__ for row in rows_to_generate])
    expected_classes = (['DuplicatesRowDecorator']*15 + ['MissingFieldsRowDecorator'] * 15 +
                        ['UnprocessableRecordRowDecorator'] * 20 + ['EmptyRowDecorator']*50)
    assert_that(classes).is_in(expected_classes)
