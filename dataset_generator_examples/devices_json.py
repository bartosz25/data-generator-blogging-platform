import logging

from data_generator.datasets.devices.generator import DeviceEntityGenerator
from data_generator.datasets.generation_context import DatasetGenerationContext, \
    NotBlockingDataGenerationBlocker, OneShotDatasetGenerator
from data_generator.datasets.main_generator import generate_dataset
from data_generator.io.file_system_writer import JsonFileSystemDatasetWriter

if __name__ == "__main__":
    import filesystem_writers_arguments

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)8.8s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    data_generation_context = DatasetGenerationContext(
        number_of_rows=1000,
        duplicates_percentage=0,
        missing_fields_percentage=0,
        unprocessable_rows_percentage=0,
        irregular_data_blocker=NotBlockingDataGenerationBlocker(),
        entity_generator=DeviceEntityGenerator()
    )
    generate_dataset(
        generation_controller=OneShotDatasetGenerator(),
        context=data_generation_context,
        writer=JsonFileSystemDatasetWriter(
            output_path=filesystem_writers_arguments.args.output_dir,
            clean_path=True)
    )
