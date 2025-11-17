from configparser import ConfigParser
from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer, SerializationError
from argparse import ArgumentParser, FileType
import logging
from confluent_kafka.schema_registry.rules.encryption.awskms.aws_driver import AwsKmsDriver
from confluent_kafka.schema_registry.rules.encryption.encrypt_executor import FieldEncryptionExecutor
from confluent_kafka.schema_registry.rules.encryption.localkms.local_driver import LocalKmsDriver


class PersonalData(object):
    def __init__(self, birthday, id, name, timestamp):
        self.id = id
        self.name = name
        self.birthday = birthday
        self.timestamp = timestamp


def message_to_dict(personalData, ctx):
    return dict(id=personalData.id,
                name=personalData.name,
                birthday=personalData.birthday,
                timestamp=personalData.timestamp)


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
    config = dict(config_parser['producer'])
    config.update(config_parser[args.cluster])
    topic_name = dict(config_parser['topic'])

    return config, sr_config, topic_name['topic']


# Fetch schema, initializer logger / deserializer / consumer
def init_producer_serializer(config, sr_conf, topic):
    schema_registry_client = SchemaRegistryClient(sr_conf)

    schema_str_value = schema_registry_client.get_latest_version(f"{topic}-value").schema.schema_str

    logger = logging.getLogger('CSFLEAvroProducer')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('./logs/CSFLEAvroProducer.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    producer = Producer(config, logger=logger)

    serializer_conf = {"auto.register.schemas": False, "use.latest.version": True}
    # local KMS
    rule_conf = {"secret": "my-key"}
    avro_serializer = AvroSerializer(schema_registry_client,
                                     to_dict=message_to_dict,
                                     conf=serializer_conf,
                                     rule_conf=rule_conf)

    # key_serializer = AvroSerializer(schema_registry_client,
    #                                 schema_str_key,
    #                                 conf=serializer_conf)

    key_serializer = StringSerializer('utf-8')

    return producer, avro_serializer, key_serializer


# Simple callback to report topic-partition offset a message was produced to
def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg, err))
        return
    print(f'Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')


def message_produce(avroProducer, avroSerializer, keySerializer, topic):
    try:
        producer.poll(1.0)
        for _ in range(1):
            message = PersonalData("yID", "Joe", "1990-01-01", "now")
            avroProducer.produce(topic=topic,
                                 # key=key_serializer(json.loads(key), SerializationContext(topic, MessageField.KEY)),
                                 # If using a String as a key
                                 # key=keySerializer(key, SerializationContext(topic,
                                 #                                             MessageField.KEY)),
                                 value=avroSerializer(message,
                                                      SerializationContext(topic,
                                                                           MessageField.VALUE)),
                                 on_delivery=delivery_report)

    except ValueError as v:
        print(f"Invalid input, discarding record...\n{v}")
    except KafkaError as k:
        print(f"Error: {k}")


# Main
if __name__ == '__main__':
    LocalKmsDriver.register()
    FieldEncryptionExecutor.register()

    config, sr_conf, topic_name = initialize_parser()
    producer, avro_serializer, key_serializer = init_producer_serializer(config, sr_conf, topic_name)
    message_produce(producer, avro_serializer, key_serializer, topic_name)
    producer.flush()
