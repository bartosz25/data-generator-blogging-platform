import logging

from data_generator.datasets.generation_context import DatasetGenerationContext, \
    NotBlockingDataGenerationBlocker, ContinuousDatasetGenerator
from data_generator.datasets.main_generator import generate_dataset
from data_generator.datasets.visits.generator import VisitEntityGenerator
from data_generator.io.postgresql_writer import PostgreSQLDatasetWriter

if __name__ == "__main__":
    import postgresql_arguments
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
                entity_generator=VisitEntityGenerator(start_time='2023-11-24T00:00:00Z'),
            )

    generate_dataset(generation_controller=ContinuousDatasetGenerator(), context=data_generation_context,
                     writer=PostgreSQLDatasetWriter(
                         host=postgresql_arguments.args.host,
                         dbname=postgresql_arguments.args.dbname,
                         user=postgresql_arguments.args.user,
                         password=postgresql_arguments.args.password,
                         table_name=postgresql_arguments.args.table_name,
                         table_columns=['visit_id', 'event_time', 'user_id', 'page'],
                         row_fields_to_insert=['visit_id', 'event_time', 'user_id', 'page'],
                     ))
