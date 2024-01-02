import pathlib
from concurrent.futures import Future

import yaml
from assertpy import assert_that

from data_generator.sink.kafka_writer import KafkaWriterConfiguration


def should_not_recreate_topic_with_disabled_flags():
    path = pathlib.Path(__file__).parent.absolute()
    with open('{}/kafka_writer_configuration.yaml'.format(path)) as file:
        configuration = yaml.load(file, Loader=yaml.FullLoader)

    kafka_configuration = KafkaWriterConfiguration(configuration['kafka'])

    class MockedAdmin:
        def __init__(self):
            self.topics_to_delete = []
            self.topics_to_create = []
            self.success_future = Future()
            self.success_future.set_result(True)

        def delete_topics(self, topics, **kwargs):
            self.topics_to_delete.extend(topics)
            return dict(map(lambda topic: (topic, self.success_future), topics))

        def create_topics(self, topics, **kwargs):
            self.topics_to_create.extend(list(map(lambda new_topic: new_topic.topic, topics)))
            return dict(map(lambda topic: (topic.topic, self.success_future), topics))
    mocked_admin = MockedAdmin()

    kafka_configuration.create_or_recreate_topics(mocked_admin)

    assert_that(mocked_admin.topics_to_create).is_length(2)\
        .contains_only('raw_data', 'invalid_data')
    assert_that(mocked_admin.topics_to_delete).is_length(2)\
        .contains_only('raw_data', 'invalid_data')
