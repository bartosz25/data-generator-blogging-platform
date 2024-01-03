import logging

from data_generator.datasets.dataset_generators import OneShotDatasetGenerationController
from data_generator.datasets.generation_context import DatasetGenerationContext, \
    NotBlockingDataGenerationBlocker
from data_generator.datasets.main_generator import generate_dataset
from data_generator.datasets.visits.generator import VisitEntityGenerator
from data_generator.io.file_system_writer import JsonFileSystemDatasetWriter

if __name__ == "__main__":
    import filesystem_partitioned_writers_arguments

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)8.8s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    partitions_to_generate = filesystem_partitioned_writers_arguments.args.partitions.split(',')

    data_generation_context = DatasetGenerationContext(
        number_of_rows=1000,
        duplicates_percentage=0,
        missing_fields_percentage=0,
        unprocessable_rows_percentage=0,
        irregular_data_blocker=NotBlockingDataGenerationBlocker(),
        entity_generator=VisitEntityGenerator(start_time='2023-11-24T00:00:00Z'),
    )
    for partition in partitions_to_generate:
        logging.info(f'Generating partition {partition}')
        output_path = f'{filesystem_partitioned_writers_arguments.args.output_dir}/{partition}'
        generate_dataset(
            generation_controller=OneShotDatasetGenerationController(),
            context=data_generation_context,
            writer=JsonFileSystemDatasetWriter(
                output_path=output_path,
                clean_path=True)
        )
