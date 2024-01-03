import argparse
import logging
from data_generator.datasets.dataset_generation_context_parser import YamlDatasetGenerationContextParser
from data_generator.datasets.main_generator import generate_dataset

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)8.8s] %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(prog='Dataset generator')
    parser.add_argument('--config_file', required=True)
    args = parser.parse_args()

    config_parser = YamlDatasetGenerationContextParser(configuration_file_path=args.config_file)
    for writer in config_parser.writers:
        generate_dataset(
            generation_controller=config_parser.generator(),
            context=config_parser.data_generation_context,
            writer=writer
        )
