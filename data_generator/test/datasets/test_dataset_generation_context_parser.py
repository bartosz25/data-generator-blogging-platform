import datetime
import pathlib

from assertpy import assert_that

from data_generator.datasets.dataset_generation_context_parser import YamlDatasetGenerationContextParser


def should_generate_context_for_constructors_without_arguments():
    path = pathlib.Path(__file__).parent.absolute()
    config_path = f'{path}/dataset_generation_context_parser_no_config_constructors.yaml'

    config_parser = YamlDatasetGenerationContextParser(configuration_file_path=config_path)

    assert_that(type(config_parser.generator()).__name__).is_equal_to('OneShotDatasetGenerationController')
    assert_that(config_parser.writers).is_length(1)
    assert_that(type(config_parser.writers[0]).__name__).is_equal_to('JsonFileSystemDatasetWriter')
    assert_that(config_parser.writers[0].output_path_with_file).is_equal_to('/tmp/abc/dataset.json')
    context = config_parser.data_generation_context.__dict__
    assert_that(context['number_of_rows']).is_equal_to(1000)
    assert_that(context['duplicates_percentage']).is_equal_to(25)
    assert_that(context['missing_fields_percentage']).is_equal_to(31)
    assert_that(context['unprocessable_rows_percentage']).is_equal_to(15)
    (assert_that(type(config_parser.data_generation_context.irregular_data_blocker).__name__)
     .is_equal_to('NotBlockingDataGenerationBlocker'))
    (assert_that(type(config_parser.data_generation_context.entity_generator).__name__)
     .is_equal_to('DeviceEntityGenerator'))


def should_generate_context_for_constructors_with_arguments():
    path = pathlib.Path(__file__).parent.absolute()
    config_path = f'{path}/dataset_generation_context_parser_with_config_constructors.yaml'

    config_parser = YamlDatasetGenerationContextParser(configuration_file_path=config_path)

    assert_that(type(config_parser.generator()).__name__).is_equal_to('FixedTimesDatasetGenerationController')
    assert_that(config_parser.generator().remaining_runs).is_equal_to(4)
    assert_that(config_parser.writers).is_length(1)
    assert_that(type(config_parser.writers[0]).__name__).is_equal_to('KafkaDatasetWriter')
    assert_that(config_parser.writers[0].output_topic).is_equal_to('visits')
    context = config_parser.data_generation_context.__dict__
    assert_that(context['number_of_rows']).is_equal_to(1000)
    assert_that(context['duplicates_percentage']).is_equal_to(25)
    assert_that(context['missing_fields_percentage']).is_equal_to(31)
    assert_that(context['unprocessable_rows_percentage']).is_equal_to(0)
    (assert_that(type(config_parser.data_generation_context.irregular_data_blocker).__name__)
     .is_equal_to('BlockingDataGenerationBlocker'))
    (assert_that(config_parser.data_generation_context.irregular_data_blocker.sleep_time_range_seconds)
     .is_equal_to(range(1, 6)))
    (assert_that(type(config_parser.data_generation_context.entity_generator).__name__)
     .is_equal_to('VisitEntityGenerator'))
    (assert_that(config_parser.data_generation_context.entity_generator.start_time_as_datetime)
     .is_equal_to(datetime.datetime(year=2023, month=11, day=24, hour=0, minute=0, second=0,
                                    tzinfo=datetime.timezone.utc)))


def should_generate_context_for_partitioned_configuration_file():
    path = pathlib.Path(__file__).parent.absolute()
    config_path = f'{path}/dataset_generation_context_parser_partitioned.yaml'

    config_parser = YamlDatasetGenerationContextParser(configuration_file_path=config_path)

    assert_that(type(config_parser.generator()).__name__).is_equal_to('OneShotDatasetGenerationController')
    assert_that(config_parser.writers).is_length(2)
    assert_that(type(config_parser.writers[0]).__name__).is_equal_to('JsonFileSystemDatasetWriter')
    assert_that(config_parser.writers[0].output_path_with_file).is_equal_to('/tmp/abc/date=2023-11-01/dataset.json')
    assert_that(type(config_parser.writers[1]).__name__).is_equal_to('JsonFileSystemDatasetWriter')
    assert_that(config_parser.writers[1].output_path_with_file).is_equal_to('/tmp/abc/date=2023-11-02/dataset.json')
    context = config_parser.data_generation_context.__dict__
    assert_that(context['number_of_rows']).is_equal_to(1000)
    assert_that(context['duplicates_percentage']).is_equal_to(25)
    assert_that(context['missing_fields_percentage']).is_equal_to(31)
    assert_that(context['unprocessable_rows_percentage']).is_equal_to(15)
    (assert_that(type(config_parser.data_generation_context.irregular_data_blocker).__name__)
     .is_equal_to('NotBlockingDataGenerationBlocker'))
    (assert_that(type(config_parser.data_generation_context.entity_generator).__name__)
     .is_equal_to('DeviceEntityGenerator'))
