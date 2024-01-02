import logging

from data_generator.datasets.generation_context import DatasetGenerationContext, \
    NotBlockingDataGenerationBlocker, ContinuousDatasetGenerator
from data_generator.datasets.main_generator import generate_dataset
from data_generator.datasets.visits.generator import VisitEntityGenerator
from data_generator.io.kafka_writer import KafkaDatasetWriter

if __name__ == "__main__":
    import kafka_arguments
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

    generate_dataset(
        generator=ContinuousDatasetGenerator(),
        context=data_generation_context,
        writer=KafkaDatasetWriter(
            broker=kafka_arguments.args.broker, extra_producer_config={'queue.buffering.max.ms': 2000},
            output_topic=kafka_arguments.args.topic
        )
    )
