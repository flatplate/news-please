import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class KafkaProducerSink(ExtractedInformationStorage):

    def __init__(self):
        super().__init__()
        self.kafka_config = self.cfg.section("Kafka")
        self.bootstrap_servers = self.kafka_config.get("bootstrap_servers")
        self.topic = self.kafka_config.get("topic", "newsplease")
        self.producer_close_timeout = self.kafka_config.get("producer_close_timeout", 1.0)
        self.kafka_version = tuple(self.kafka_config.get("api_version", [3, 0, 0]))
        try:
            self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                          value_serializer=self.default_json_serializer,
                                          api_version=self.kafka_version)
        except KafkaError:
            self.log.exception("Couldn't initialize kafka producer, will continue without the producer.")
            self.producer = None

    def process_item(self, item, spider):
        if self.producer is None:
            return item
        item_dict = self.extract_relevant_info(item)
        try:
            self.producer.send(self.topic, value=item_dict)
        except KafkaError:
            self.log.exception("Could not push article into the kafka topic")
        return item

    def close_spider(self):
        self.producer.flush()
        self.producer.close(timeout=self.producer_close_timeout)

    @staticmethod
    def default_json_serializer(value):
        return json.dumps(value).encode('utf-8')