import dataclasses
import logging
import random
import time
from abc import ABC, abstractmethod
from math import floor
from typing import List

from data_generator.datasets.data_generator_entity import EntityGenerator
from data_generator.datasets.quality_issues import RowDecorator, DuplicatesRowDecorator, MissingFieldsRowDecorator, \
    UnprocessableRecordRowDecorator, EmptyRowDecorator
from data_generator.io.dataset_writer import DatasetWriter


class DataGenerationBlocker(ABC):
    @abstractmethod
    def block(self): raise NotImplementedError


class NotBlockingDataGenerationBlocker(DataGenerationBlocker):

    def block(self):
        pass


class BlockingDataGenerationBlocker(DataGenerationBlocker):

    def __init__(self, sleep_time_range_seconds: range):
        self.sleep_time_range_seconds = sleep_time_range_seconds

    def block(self):
        sleep_time = random.randrange(self.sleep_time_range_seconds.start, self.sleep_time_range_seconds.stop)
        logging.info(f'Sleeping for {sleep_time} seconds')
        time.sleep(sleep_time)


@dataclasses.dataclass
class DatasetGenerationContext:
    number_of_rows: int
    irregular_data_blocker: DataGenerationBlocker
    entity_generator: EntityGenerator
    duplicates_percentage: int = 0
    missing_fields_percentage: int = 0
    unprocessable_rows_percentage: int = 0
    late_rows_percentage: int = 0

    def get_rows_to_generate_with_maybe_decorators(self) -> List[RowDecorator]:
        rows_to_generate = self.number_of_rows
        logging.info(f'Will be generating {rows_to_generate} rows...')

        number_of_duplicates = DatasetGenerationContext.calculate_percentage(rows_to_generate,
                                                                             self.duplicates_percentage)
        logging.info(f'...{number_of_duplicates} are duplicates...')
        decorators = [DuplicatesRowDecorator(self.entity_generator.generate_row, i)
                      for i in range(0, number_of_duplicates)]

        number_of_missing_fields = DatasetGenerationContext.calculate_percentage(rows_to_generate,
                                                                                 self.missing_fields_percentage)
        logging.info(f'...{number_of_missing_fields} have missing fields...')
        decorators[len(decorators):] = [MissingFieldsRowDecorator(self.entity_generator.generate_row, i)
                                        for i in range(len(decorators), len(decorators) + number_of_missing_fields)]

        number_of_unprocessable_rows = DatasetGenerationContext.calculate_percentage(rows_to_generate,
                                                                                     self.unprocessable_rows_percentage)
        logging.info(f'...{number_of_unprocessable_rows} are unprocessable...')
        decorators[len(decorators):] = [UnprocessableRecordRowDecorator(self.entity_generator.generate_row, i)
                                        for i in
                                        range(len(decorators), len(decorators) + number_of_unprocessable_rows)]

        number_of_rows_without_issues = max(0,
                                            int(rows_to_generate - number_of_duplicates - number_of_missing_fields
                                                - number_of_unprocessable_rows))
        logging.info(f'...{number_of_rows_without_issues} don''t have any issues...')
        decorators[len(decorators):] = [EmptyRowDecorator(self.entity_generator.generate_row, i)
                                        for i in
                                        range(len(decorators), len(decorators) + number_of_rows_without_issues)]
        return decorators

    @staticmethod
    def calculate_percentage(rows_to_generate: int, percentage: int) -> int:
        return int(floor(rows_to_generate * (percentage / 100)))


@dataclasses.dataclass
class DatasetGenerationContextWithWriter:
    dataset_generation_context: DatasetGenerationContext
    writers: List[DatasetWriter]
