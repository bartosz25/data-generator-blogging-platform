from typing import Dict, Any, List, Callable, Optional

import yaml

from data_generator.datasets.data_generator_entity import EntityGenerator
from data_generator.datasets.dataset_generators import (DatasetGenerationController, OneShotDatasetGenerationController,
                                                        FixedTimesDatasetGenerationController,
                                                        ContinuousDatasetGenerationController)
from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.generation_context import NotBlockingDataGenerationBlocker, \
    BlockingDataGenerationBlocker, DatasetGenerationContext, DataGenerationBlocker
from data_generator.datasets.users.generator import RegisteredUserEntityGenerator
from data_generator.datasets.visits.generator import VisitEntityGenerator
from data_generator.io.dataset_writer import DatasetWriter
from data_generator.io.file_system_writer import CsvFileSystemWriter, JsonFileSystemDatasetWriter
from data_generator.io.kafka_writer import KafkaDatasetWriter
from data_generator.io.postgresql_writer import PostgreSQLDatasetWriter


class YamlDatasetGenerationContextParser:

    def __init__(self, configuration_file_path: str):
        with open(configuration_file_path) as file:
            configuration: Dict[str, Any] = yaml.load(file, Loader=yaml.FullLoader)

            data_blocker = YamlDatasetGenerationContextParser._get_data_blocker(configuration['data_blocker'])
            entity_generator = YamlDatasetGenerationContextParser._get_entity_generator(configuration['entity'])
            dataset_config = configuration['dataset']
            dataset_config_composition = dataset_config['composition_percentage']
            self._data_generation_context = DatasetGenerationContext(
                number_of_rows=dataset_config['rows'],
                duplicates_percentage=YamlDatasetGenerationContextParser._get_data_composition_or_default(
                    dataset_config_composition, 'duplicates'),
                missing_fields_percentage=YamlDatasetGenerationContextParser._get_data_composition_or_default(
                    dataset_config_composition, 'missing_fields'),
                unprocessable_rows_percentage=YamlDatasetGenerationContextParser._get_data_composition_or_default(
                    dataset_config_composition, 'unprocessable_rows'),
                late_rows_percentage=YamlDatasetGenerationContextParser._get_data_composition_or_default(
                    dataset_config_composition, 'late_rows_percentage'),
                irregular_data_blocker=data_blocker,
                entity_generator=entity_generator
            )
            self._generator = lambda: YamlDatasetGenerationContextParser._get_generator(configuration['generator'])
            self._writers = YamlDatasetGenerationContextParser._get_writers(configuration['writer'])

    @property
    def data_generation_context(self) -> DatasetGenerationContext:
        return self._data_generation_context

    @property
    def generator(self) -> Callable[[], DatasetGenerationController]:
        return self._generator

    @property
    def writers(self) -> List[DatasetWriter]:
        return self._writers

    @staticmethod
    def _get_data_composition_or_default(composition_config: Dict[str, Optional[int]], attribute: str) -> int:
        if attribute in composition_config:
            return composition_config[attribute]
        else:
            return 0

    @staticmethod
    def _get_data_blocker(data_blocker: Dict[str, Any]) -> DataGenerationBlocker:
        blocker_type = data_blocker['type']
        blocker_types = {
            'no': lambda blocker_config: NotBlockingDataGenerationBlocker(),
            'sleep': lambda blocker_config: BlockingDataGenerationBlocker(sleep_time_range_seconds=range(
                blocker_config['sleep_time_range_seconds']['from'],
                blocker_config[
                    'sleep_time_range_seconds']['to'])
            )
        }
        blocker_configuration = data_blocker['configuration'] if 'configuration' in data_blocker else {}
        return blocker_types[blocker_type](blocker_configuration)

    @staticmethod
    def _get_entity_generator(entity: Dict[str, Any]) -> EntityGenerator:
        entity_type = entity['type']
        entity_types = {
            'device': lambda entity_config: DeviceEntityGenerator(),
            'user': lambda entity_config: RegisteredUserEntityGenerator(),
            'visit': lambda entity_config: VisitEntityGenerator(**entity_config),
        }
        entity_configuration = entity['configuration'] if 'configuration' in entity else {}
        return entity_types[entity_type](entity_configuration)

    @staticmethod
    def _get_generator(generator: Dict[str, Any]) -> DatasetGenerationController:
        generator_type = generator['type']
        generator_types = {
            'one-shot': lambda config: OneShotDatasetGenerationController(),
            'fixed-times': lambda config: FixedTimesDatasetGenerationController(**config),
            'continuous': lambda config: ContinuousDatasetGenerationController()
        }
        generator_configuration = generator['configuration'] if 'configuration' in generator else {}
        return generator_types[generator_type](generator_configuration)

    @staticmethod
    def _get_writers(writer: Dict[str, Any]) -> List[DatasetWriter]:
        writer_type = writer['type']
        writer_types = {
            'json': lambda config: JsonFileSystemDatasetWriter(**config),
            'csv': lambda config: CsvFileSystemWriter(**config),
            'kafka': lambda config: KafkaDatasetWriter(**config),
            'postgresql': lambda config: PostgreSQLDatasetWriter(**config)
        }
        if 'partitions' in writer:
            writers = []
            for partition in writer['partitions']:
                if 'output_path' in writer['configuration']:
                    new_output_path = f"{writer['configuration']['output_path']}/{partition}"
                    config_to_apply = {**writer['configuration'], **{'output_path': new_output_path}}
                    writers.append(writer_types[writer_type](config_to_apply))
            return writers
        else:
            return [writer_types[writer_type](writer['configuration'])]
