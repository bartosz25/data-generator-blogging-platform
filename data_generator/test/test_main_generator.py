import json
import os
import shutil

from assertpy import assert_that

from data_generator.datasets.dataset_generators import OneShotDatasetGenerationController
from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.generation_context import DatasetGenerationContext, NotBlockingDataGenerationBlocker
from data_generator.datasets.main_generator import generate_dataset
from data_generator.io.file_system_writer import JsonFileSystemDatasetWriter


def should_generate_datasets_without_reference_datasets():
    output_path = '/tmp/test-1'
    shutil.rmtree(output_path, ignore_errors=True)
    os.makedirs(output_path, exist_ok=True)
    generate_dataset(
        generation_controller=OneShotDatasetGenerationController(),
        context=DatasetGenerationContext(
            number_of_rows=5,
            irregular_data_blocker=NotBlockingDataGenerationBlocker(),
            entity_generator=DeviceEntityGenerator()
        ),
        writer=JsonFileSystemDatasetWriter(clean_path=True, output_path=output_path)
    )

    with open(f'{output_path}/dataset.json') as json_file:
        all_rows = 0
        for line in json_file.readlines():
            json_data = json.loads(line.strip())
            assert_that(json_data['type']).is_not_empty()
            assert_that(json_data['full_name']).is_not_empty()
            assert_that(json_data['version']).is_not_empty()
            all_rows += 1
        assert_that(all_rows).is_equal_to(5)
