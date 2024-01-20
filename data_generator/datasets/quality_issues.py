import logging
import random
import types
from abc import abstractmethod, ABC
from queue import Queue
from typing import List, Callable

from data_generator.datasets.data_generator_entity import DataGeneratorEntity


class RowDecorator(ABC):

    def __init__(self, decorator_method: Callable[[int], DataGeneratorEntity], entity_index_from_decorator: int):
        self.decorator_method = decorator_method
        self.entity_index_from_decorator = entity_index_from_decorator

    def return_decorated_row(self):
        row_to_decorate = self.decorator_method(self.entity_index_from_decorator)
        return self.decorate(row_to_decorate)

    @abstractmethod
    def decorate(self, row_to_decorate: DataGeneratorEntity) -> List[DataGeneratorEntity]: raise NotImplementedError


class DuplicatesRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        should_be_duplicated = random.choices(population=[True, False], weights=[0.3, 0.6])[0]
        if should_be_duplicated:
            logging.debug(f'{row_to_decorate} will be duplicated')
            return [row_to_decorate, row_to_decorate]
        else:
            return [row_to_decorate]


class MissingFieldsRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        callable_types = types.FunctionType, types.MethodType
        all_callable_fields = ([key for key, value in row_to_decorate.__dict__.items()
                                if not isinstance(value, callable_types)])

        nullable_fields = random.sample(all_callable_fields, random.randint(1, len(all_callable_fields)))
        for nullable_field_attribute in nullable_fields:
            setattr(row_to_decorate, nullable_field_attribute, None)
        return [row_to_decorate]


class UnprocessableRecordRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        # TODO: check if it works every time; maybe better to do sth else, like generating the value or altering
        #       the first/last byte
        return ['{........']


class EmptyRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        return [row_to_decorate]


class LateRowDecorator(RowDecorator):
    DEQUEUE_DISTRIBUTION = [False] * 92 + [True] * 8

    def __init__(self, decorator_method: Callable[[int], DataGeneratorEntity], entity_index_from_decorator: int,
                 max_late_rows: int = 100000):
        super().__init__(decorator_method, entity_index_from_decorator)
        self.__late_rows = Queue(maxsize=max_late_rows)

    def decorate(self, row_to_decorate: DataGeneratorEntity) -> List[DataGeneratorEntity]:
        should_dequeue = random.choice(LateRowDecorator.DEQUEUE_DISTRIBUTION) or self.__late_rows.full()
        rows_to_return = []
        if should_dequeue:
            rows_to_return_number = min(random.randint(1, 50), self.__late_rows.qsize())
            logging.info(f'Flushing {rows_to_return_number} of {self.__late_rows.qsize()} late records')
            for _ in range(0, rows_to_return_number):
                rows_to_return.append(self.__late_rows.get())
        # Full Exception should happen as we dequeue when the queue is full, hence we should always have at least one
        # spot available
        self.__late_rows.put(row_to_decorate)

        return rows_to_return
