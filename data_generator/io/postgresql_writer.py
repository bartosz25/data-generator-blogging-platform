import json
from typing import List

import psycopg2
from psycopg2._psycopg import AsIs
from psycopg2.extensions import register_adapter

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.io.converters import json_converter
from data_generator.io.dataset_writer import DatasetWriter


class PostgreSQLDatasetWriter(DatasetWriter):

    def __init__(self, host: str, dbname: str, db_schema: str, user: str, password: str, table_name: str,
                 table_columns: List[str], row_fields_to_insert: List[str]):
        connection_uri = f'host={host} dbname={dbname} user={user} password={password}'
        self.postgresql_connection = psycopg2.connect(connection_uri)
        self.postgresql_cursor = self.postgresql_connection.cursor()
        self.table_full_path = f'{db_schema}.{table_name}'
        self.table_columns_as_text = ', '.join(table_columns)
        self.insert_placeholders = ', '.join(['%s' for i in table_columns])
        self.row_fields_to_insert = row_fields_to_insert

        # this adapter is required for JSONB types
        def adapt_dict(dict_var):
            return AsIs("'" + json.dumps(dict_var, default=json_converter) + "'")

        register_adapter(dict, adapt_dict)

    def write_dataset_decorated_rows(self, decorated_rows: List[DataGeneratorEntity]):
        for row in decorated_rows:
            row_as_dict = row.as_dict()
            values_to_insert = [row_as_dict[key] for key in self.row_fields_to_insert]

            self.postgresql_cursor.execute(f'INSERT INTO {self.table_full_path} ({self.table_columns_as_text}) '
                                           f'VALUES ({self.insert_placeholders})', values_to_insert)

    def flush(self):
        self.postgresql_connection.commit()
