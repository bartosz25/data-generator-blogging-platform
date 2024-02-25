import argparse
import logging
import sys

from data_generator.datasets.dataset_generation_context_parser import YamlDatasetGenerationContextParser
from data_generator.datasets.dataset_generators import OneShotDatasetGenerationController
from data_generator.datasets.main_generator import generate_dataset


def generate_datasets_from_config_file(config_file_path: str):
    config_parser = YamlDatasetGenerationContextParser(configuration_file_path=config_file_path)
    # If there are any Reference Datasets, generate them before because they may be referenced by the dataset below
    for reference_dataset_context in config_parser.reference_datasets_contexts.values():
        for writer in reference_dataset_context.writers:
            generate_dataset(
                generation_controller=OneShotDatasetGenerationController(),
                context=reference_dataset_context.dataset_generation_context,
                writer=writer
            )

    for writer in config_parser.writers:
        generate_dataset(
            generation_controller=config_parser.generator(),
            context=config_parser.data_generation_context,
            writer=writer
        )


def main(input_args):
    parser = argparse.ArgumentParser(prog='Dataset generator')
    parser.add_argument('--config_file', required=True)
    args = parser.parse_args(input_args)
    generate_datasets_from_config_file(args.config_file)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)8.8s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    main(sys.argv[1:])
