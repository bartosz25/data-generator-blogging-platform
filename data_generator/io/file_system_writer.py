import csv
import dataclasses
import json
import logging
import os
import shutil
from pathlib import Path
from typing import List

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.io.converters import json_converter
from data_generator.io.dataset_writer import DatasetWriter


class JsonFileSystemDatasetWriter(DatasetWriter):

    def __init__(self, output_path: str, clean_path: bool = False):
        if clean_path:
            shutil.rmtree(path=output_path, ignore_errors=True)

        os.makedirs(output_path, exist_ok=True)
        self.output_path_with_file = f'{output_path}/dataset.json'
        self.rows_to_write = []

    def write_dataset_decorated_rows(self, decorated_rows: List[DataGeneratorEntity]):
        for row in decorated_rows:
            self.rows_to_write.append(row)
            if len(self.rows_to_write) == 100:
                self._flush_partial_dataset()

    def flush(self):
        self._flush_partial_dataset()

    def _flush_partial_dataset(self):
        with open(self.output_path_with_file, 'a') as dataset_partial_file:
            for row in self.rows_to_write:
                if type(row) is str:
                    dataset_partial_file.write(row)
                else:
                    dataset_partial_file.write(json.dumps(row.as_dict(), default=json_converter))
                dataset_partial_file.write('\n')


class CsvFileSystemWriter(DatasetWriter):
    def __init__(self, output_path: str, clean_path: bool = False):
        if clean_path:
            shutil.rmtree(path=output_path, ignore_errors=True)
        Path(output_path).mkdir(parents=True, exist_ok=True)

        self.output_path_with_file = f'{output_path}/dataset.csv'
        self.rows_to_write = []
        self.header_written = False

    def write_dataset_decorated_rows(self, decorated_rows: List[DataGeneratorEntity]):
        for row in decorated_rows:
            self.rows_to_write.append(row)
            if len(self.rows_to_write) == 100:
                self._flush_partial_dataset()

    def flush(self):
        self._flush_partial_dataset()

    def _flush_partial_dataset(self):
        csv_header = list(self.rows_to_write[0][0].as_dict().keys())
        logging.debug(f'Flushing to {self.output_path_with_file}')
        with open(self.output_path_with_file, 'a') as dataset_partial_file:
            visit_csv_writer = csv.DictWriter(
                dataset_partial_file, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                fieldnames=csv_header
            )
            if not self.header_written:
                visit_csv_writer.writeheader()
                self.header_written = True
            for row in self.rows_to_write:
                if type(row) is str:
                    dataset_partial_file.write(row)
                    dataset_partial_file.write('\n')
                else:
                    visit_csv_writer.writerow(row.as_dict())