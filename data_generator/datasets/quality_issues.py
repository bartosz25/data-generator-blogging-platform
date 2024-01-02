import logging
import random
import types
from abc import abstractmethod, ABC
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
        has_missing_fields = False
        for key, value in row_to_decorate.__dict__.items():
            should_be_missing = random.choices(population=[True, False], weights=[0.3, 0.8])[0]
            if not isinstance(value, callable_types) and should_be_missing:
                setattr(row_to_decorate, key, None)
                has_missing_fields = True
        if has_missing_fields:
            logging.debug(f'{row_to_decorate} has missing fields')
        return [row_to_decorate]


class UnprocessableRecordRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        # TODO: check if it works every time; maybe better to do sth else, like generating the value or altering
        #       the first/last byte
        return ['{........']


class EmptyRowDecorator(RowDecorator):

    def decorate(self, row_to_decorate: DataGeneratorEntity):
        return [row_to_decorate]
