import json

from kafka import KafkaProducer

from newsplease.pipeline.pipelines.elements.extracted_information_storage import ExtractedInformationStorage


class KafkaProducerSink(ExtractedInformationStorage):

    def __init__(self):
        super().__init__()
        self.kafka_config = self.cfg.section("Kafka")
        self.bootstrap_servers = self.kafka_config.get("bootstrap_servers")
        self.topic = self.kafka_config.get("topic", "newsplease")
        self.kafka_version = tuple(self.kafka_config.get("api_version", [3, 0, 0]))
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=self.default_json_serializer,
                                      api_version=self.kafka_version)

    def process_item(self, item, spider):
        item_dict = self.extract_relevant_info(item)
        self.producer.send(self.topic, value=item_dict)
        return item

    def close_spider(self):
        self.producer.flush()
        self.producer.close()

    @staticmethod
    def default_json_serializer(value):
        return json.dumps(value).encode('utf-8')