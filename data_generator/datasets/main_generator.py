import logging

from data_generator.datasets.dataset_generators import DatasetGenerationController
from data_generator.datasets.generation_context import DatasetGenerationContext
from data_generator.io.dataset_writer import DatasetWriter


def generate_dataset(generation_controller: DatasetGenerationController, context: DatasetGenerationContext,
                     writer: DatasetWriter):
    rows_to_generate = context.get_rows_to_generate_with_maybe_decorators()
    while generation_controller.should_continue():
        for row_decorator in rows_to_generate:
            row = row_decorator.return_decorated_row()
            logging.debug(f'Generated row is {row}')
            writer.write_dataset_decorated_rows(row)
        writer.flush()

        context.irregular_data_blocker.block()


