import json
import logging
from typing import Any, Dict, List

from confluent_kafka import Producer

from data_generator.datasets.data_generator_entity import DataGeneratorEntity
from data_generator.io.dataset_writer import DatasetWriter


class KafkaDatasetWriter(DatasetWriter):

    def __init__(self, broker: str, output_topic: str, extra_producer_config: Dict[str, Any]):
        # check https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for more information
        producer_config = {
            **{'bootstrap.servers': broker},
            **extra_producer_config
        }
        self.producer = Producer(producer_config)
        self.output_topic = output_topic

    def write_dataset_decorated_rows(self, decorated_rows: List[DataGeneratorEntity]):
        """
        Traceback (most recent call last):
        File "/home/bartosz/workspace/data-generator/dataset_examples/kafka/generate_dataset_to_kafka.py", line 51, in <module>
            configuration.send_message(output_topic_name, action)
        File "/home/bartosz/workspace/data-generator/data_generator/sink/kafka_writer.py", line 60, in send_message
            self.producer.produce(topic_name, value=bytes(message, encoding='utf-8'))
        BufferError: Local: Queue full
        The below code is the workaround for the above problem, found here:
        `Confluent Kafka-Python issue 104 <https://github.com/confluentinc/confluent-kafka-python/issues/104>`
        """
        for row in decorated_rows:
            try:
                self._produce_message(row)
            except BufferError:
                self.producer.flush()
                self._produce_message(row)

    def _produce_message(self, row: DataGeneratorEntity):
        def delivery_callback(error, result):
            if error:
                logging.error("Record was not correctly delivered: %s", error)

        event_key = row.partition_key()
        event_json = json.dumps(row.as_dict())
        self.producer.produce(topic=self.output_topic, key=bytes(event_key, encoding='utf-8'),
                              value=bytes(event_json, encoding='utf-8'),
                              on_delivery=delivery_callback)

    def flush(self):
        self.producer.flush()
