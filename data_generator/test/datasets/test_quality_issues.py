from typing import Dict, Any

from assertpy import assert_that

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.datasets.quality_issues import EmptyRowDecorator, DuplicatesRowDecorator, \
    UnprocessableRecordRowDecorator, MissingFieldsRowDecorator, LateRowDecorator


class DummyDataEntity(DataGeneratorEntity):

    def __init__(self):
        self.a = 'a'
        self.b = 'b'

    def as_dict(self) -> Dict[str, Any]:
        pass

    def entity_partition_key(self) -> str:
        pass


def should_return_input_record_for_EmptyRowDecorator():
    row = DummyDataEntity()
    decorator = EmptyRowDecorator(lambda x: row, 1)

    decorated_rows = decorator.return_decorated_row()

    assert_that(decorated_rows).is_length(1)
    assert_that(decorated_rows).contains_only(row)


def should_return_duplicated_input_record_for_DuplicatesRowDecorator():
    row = DummyDataEntity()
    decorator = DuplicatesRowDecorator(lambda x: row, 1)

    # it may be duplicated from time to time, so run at most 100 times; logically we should have the duplicate
    # sometime
    decorated_rows = decorator.return_decorated_row()

    for i in range(0, 100):
        if len(decorated_rows) == 2:
            assert_that(decorated_rows).is_length(2)
            assert_that(decorated_rows).contains_only(row)
            break


def should_return_unprocessable_input_record_for_UnprocessableRecordRowDecorator():
    row = DummyDataEntity()
    decorator = UnprocessableRecordRowDecorator(lambda x: row, 1)

    decorated_rows = decorator.return_decorated_row()

    assert_that(decorated_rows).is_length(1)
    assert_that(decorated_rows).contains_only('{........')


def should_return_input_record_with_missing_fields_for_MissingFieldsRowDecorator():
    decorator = MissingFieldsRowDecorator(lambda x: DummyDataEntity(), 1)

    decorated_rows = decorator.return_decorated_row()
    assert_that(decorated_rows).is_length(1)
    has_missing_fields = False
    if decorated_rows[0].a is None or decorated_rows[0].b is None:
        has_missing_fields = True

    assert_that(has_missing_fields, 'Missing fields on the tested decorator').is_true()


def should_return_row_late():
    decorator = LateRowDecorator(lambda x: DummyDataEntity(), 1, 10)

    assert_that(decorator._LateRowDecorator__late_rows.qsize()).is_equal_to(0)

    decorator.decorate(DummyDataEntity())

    assert_that(decorator._LateRowDecorator__late_rows.qsize()).is_equal_to(1)

    decorated_late_rows = []
    for _ in range(1, 100):
        decorated_late_rows += decorator.return_decorated_row()

    assert_that(decorated_late_rows).is_not_empty()
