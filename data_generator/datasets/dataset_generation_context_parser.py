from typing import Dict, Any, List, Callable, Optional

import yaml

from data_generator.datasets.data_generator_entity import EntityGenerator
from data_generator.datasets.dataset_generators import (DatasetGenerationController, OneShotDatasetGenerationController,
                                                        FixedTimesDatasetGenerationController,
                                                        ContinuousDatasetGenerationController)
from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.generation_context import NotBlockingDataGenerationBlocker, \
    BlockingDataGenerationBlocker, DatasetGenerationContext, DataGenerationBlocker, DatasetGenerationContextWithWriter
from data_generator.datasets.users.generator import RegisteredUserEntityGenerator
from data_generator.datasets.visits.devices_providers import RandomTechnicalContextProvider, \
    DeviceTechnicalContextProvider
from data_generator.datasets.visits.generator import VisitEntityGenerator
from data_generator.datasets.visits.users_providers import RandomUserContextProvider, UserContextWithUserProvider
from data_generator.io.dataset_writer import DatasetWriter
from data_generator.io.file_system_writer import CsvFileSystemWriter, JsonFileSystemDatasetWriter
from data_generator.io.kafka_writer import KafkaDatasetWriter
from data_generator.io.postgresql_writer import PostgreSQLDatasetWriter


class YamlDatasetGenerationContextParser:

    def __init__(self, configuration_file_path: str):
        with open(configuration_file_path) as file:
            configuration: Dict[str, Any] = yaml.load(file, Loader=yaml.FullLoader)

            self._reference_datasets_contexts = self._get_reference_datasets_context(configuration)
            data_blocker = self._get_data_blocker(configuration['data_blocker'])
            self._data_generation_context = self.prepare_dataset_generation_context(configuration, data_blocker)
            self._generator = lambda: self._get_generator(configuration['generator'])
            self._writers = self._get_writers(configuration['writer'])

    def prepare_dataset_generation_context(self, configuration: Dict[str, Any],
                                           data_blocker: DataGenerationBlocker) -> DatasetGenerationContext:
        entity_generator = self._get_entity_generator(configuration['entity'])
        dataset_config = configuration['dataset']
        dataset_config_composition = ([] if 'composition_percentage' not in dataset_config
                                      else dataset_config['composition_percentage'])
        return DatasetGenerationContext(
            number_of_rows=dataset_config['rows'],
            duplicates_percentage=self._get_data_composition_or_default(
                dataset_config_composition, 'duplicates'),
            missing_fields_percentage=self._get_data_composition_or_default(
                dataset_config_composition, 'missing_fields'),
            unprocessable_rows_percentage=self._get_data_composition_or_default(
                dataset_config_composition, 'unprocessable_rows'),
            late_rows_percentage=self._get_data_composition_or_default(
                dataset_config_composition, 'late_rows_percentage'),
            irregular_data_blocker=data_blocker,
            entity_generator=entity_generator
        )

    @property
    def data_generation_context(self) -> DatasetGenerationContext:
        return self._data_generation_context

    @property
    def generator(self) -> Callable[[], DatasetGenerationController]:
        return self._generator

    @property
    def writers(self) -> List[DatasetWriter]:
        return self._writers

    @property
    def reference_datasets_contexts(self) -> Dict[str, DatasetGenerationContextWithWriter]:
        return self._reference_datasets_contexts

    def _get_reference_datasets_context(self, configuration: Dict[str, List[Any]]) \
            -> Dict[str, DatasetGenerationContextWithWriter]:
        contexts = {}
        if 'reference_datasets' not in configuration:
            return contexts

        for reference_dataset in configuration['reference_datasets']:
            blocker = NotBlockingDataGenerationBlocker()
            dataset_generation_context = self.prepare_dataset_generation_context(reference_dataset,
                                                                                 data_blocker=blocker)
            writers = self._get_writers(reference_dataset['writer'])
            contexts[reference_dataset['reference_key']] = DatasetGenerationContextWithWriter(
                dataset_generation_context=dataset_generation_context, writers=writers
            )
        return contexts

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

    def _get_entity_generator(self, entity: Dict[str, Any]) -> EntityGenerator:
        entity_type = entity['type']

        def prepare_visit_generator(entity_config: Dict[str, Any]) -> VisitEntityGenerator:
            technical_context_provider = RandomTechnicalContextProvider()
            user_context_provider = RandomUserContextProvider()
            if 'reference_datasets' in entity:
                reference_datasets_configuration = entity['reference_datasets']
                if 'users' in reference_datasets_configuration:
                    users_reference_dataset_key = reference_datasets_configuration['users']
                    users_generator = (self._reference_datasets_contexts[users_reference_dataset_key]
                                       .dataset_generation_context.entity_generator)
                    user_context_provider = UserContextWithUserProvider(users_generator)
                if 'devices' in reference_datasets_configuration:
                    devices_reference_dataset_key = reference_datasets_configuration['devices']
                    devices_generator = (self._reference_datasets_contexts[devices_reference_dataset_key]
                                         .dataset_generation_context.entity_generator)
                    technical_context_provider = DeviceTechnicalContextProvider(devices_generator)

            return VisitEntityGenerator(**entity_config, technical_context_provider=technical_context_provider,
                                        user_context_provider=user_context_provider)

        entity_types = {
            'device': lambda entity_config: DeviceEntityGenerator(),
            'user': lambda entity_config: RegisteredUserEntityGenerator(),
            'visit': prepare_visit_generator,
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
