from assertpy import assert_that

from data_generator.io.file_system_writer import JsonFileSystemDatasetWriter, CsvFileSystemWriter


def should_write_json_file_with_2_flushes():
    json_writer = JsonFileSystemDatasetWriter(output_path='/tmp/json_test_2_flushes_json/', clean_path=True)

    class DummyObject:
        def __init__(self, nr: int):
            self.nr = nr

        def as_dict(self):
            return {'nr': self.nr}

    json_writer.write_dataset_decorated_rows([DummyObject(1)] * 100)
    json_writer.write_dataset_decorated_rows([DummyObject(2)] * 100)
    json_writer.flush()

    with open('/tmp/json_test_2_flushes_json/dataset.json') as f:
        lines = [line.rstrip('\n') for line in f]

        assert_that(lines).is_length(200)
        assert_that(lines[0:100]).contains_only('{"nr": 1}')
        assert_that(lines[100:200]).contains_only('{"nr": 2}')


def should_write_csv_file_with_2_flushes():
    csv_writer = CsvFileSystemWriter(output_path='/tmp/json_test_2_flushes_csv/', clean_path=True)

    class DummyObject:
        def __init__(self, nr: int):
            self.nr = nr

        def as_dict(self):
            return {'nr': self.nr}

    csv_writer.write_dataset_decorated_rows([DummyObject(1)] * 100)
    csv_writer.write_dataset_decorated_rows([DummyObject(2)] * 100)
    csv_writer.flush()

    with open('/tmp/json_test_2_flushes_csv/dataset.csv') as f:
        lines = [line.rstrip('\n') for line in f]

        assert_that(lines).is_length(201)
        assert_that(lines[0]).is_equal_to('nr')
        assert_that(lines[1:101]).contains_only('1')
        assert_that(lines[101:201]).contains_only('2')
