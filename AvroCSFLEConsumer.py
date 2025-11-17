from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, record_subject_name_strategy, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from argparse import ArgumentParser, FileType
import logging
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor


# Parse the client.ini file
def initialize_parser():
    parser = ArgumentParser()
    parser.add_argument('-c', dest="cluster", required=True,
                        help="This argument is looking for the header in your ini file to determine which Kafka resource to connect to.")
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    sr_config = dict(config_parser['schemaregistry'])
    config = dict(config_parser['consumer'])
    config.update(config_parser[args.cluster])
    topic_name = dict(config_parser['topic'])

    return config, sr_config, topic_name['topic']


# Fetch schema, initializer logger / deserializer / consumer
def init_consumer_deserializer(config, topic):
    schema_registry_client = SchemaRegistryClient(sr_conf)

    schema_str_value = None

    logger = logging.getLogger('CSFLEAvroProducer')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('./logs/CSFLEAvroConsumer.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    consumer = Consumer(config, logger=logger)

    rule_conf = {"access.key.id": "id", "secret.access.key": "key"}
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                         rule_conf=rule_conf)

    # key_serializer = AvroSerializer(schema_registry_client,
    #                                 schema_str_key,
    #                                 conf=serializer_conf)

    # key_deserializer = StringSerializer('utf-8')

    return consumer, avro_deserializer


def consume(consumer, avro_deserializer, topic):
    consumer.subscribe([topic])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                continue

            try:
                message = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if msg.error() is None:
                    print(f"Message fetched from {msg.topic()}: {message}")

            except Exception as e:
                print(f"Bad message from {msg.topic()} at offset {msg.offset()}, trying string serializer\n{e}")
                break
        except KeyboardInterrupt:
            break


# Main
if __name__ == '__main__':
    AwsKmsDriver.register()
    FieldEncryptionExecutor.register()

    config, sr_conf, topic_name = initialize_parser()
    consumer, avro_deserializer = init_consumer_deserializer(config, topic_name)
    consume(consumer, avro_deserializer, topic_name)
