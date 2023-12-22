from configparser import ConfigParser
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from argparse import ArgumentParser, FileType
import json
import logging


# Stats callback, add statistics.interval.ms to your client.ini for your client to have this trigger
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    with open("./AvroConsumer_stats.log", "w") as f:
        json.dump(stats_json, f)


# Parse the client.ini file
def initialize_parser():
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['consumer'])
    # Change below to what you're connecting to, client.ini is currently set up for:
    #     CP-Demo: mTLS connection : 'cp-demo'
    #     Support's CCloud Basic Cluster: 'basic'
    #     Support's CCLoud Standard Cluster: 'standard'
    config.update(config_parser['basic'])
    topic = dict(config_parser['topic'])

    # sr_config will either be schemaregistry for CCloud or cp-schemaregistry for CP-Demo
    sr_config = dict(config_parser['schemaregistry'])
    return config, sr_config, topic['topic']


# Fetch schema, initialize logger / deserializer / consumer
def initialize_consumer_deserializer(config, sr_conf, topic):
    schema_registry_client = SchemaRegistryClient(sr_conf)

    # Look up a schema by TopicNameStrategy
    schema_str = schema_registry_client.get_latest_version(f"{topic}-value").schema.schema_str

    # Look up a schema by full name
    # schema_str = schema_registry_client.get_latest_version(":.james-context:james-context-value").schema.schema_str

    # Look up schema by ID
    # schema_str = schema_registry_client.get_schema(<ID>).schema_str

    # If you want to read a schema from a file rather than look it up, use below
    # with open("./path/to/schema/file", "r") as f:
    #    schema_str = f.read()

    # Set up logger, keep this at DEBUG and use the consumer config (debug:) to tune debug levels
    logger = logging.getLogger('consumer')
    # Can change filename field below to an absolute path, otherwise writes to pwd
    logging.basicConfig(filename='AvroConsumer.log', encoding='utf-8', level=logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # AvroDeserializer: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.schema_registry.avro.AvroDeserializer
    #     schema_registry_client (SchemaRegistryClient)
    #     schema_str (str, Schema, optional)
    #     from_dict (callable, optional)
    #     return_record_name (bool)
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Initialize consumer, ignore syntax highlighting on below logger= call
    consumer = Consumer(config, logger=logger)

    return consumer, avro_deserializer


# Subscribe to topic, consume records, print to screen
def consume(consumer, avro_deserializer, topic):
    consumer.subscribe([topic])
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                continue
            try:
                message = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            except Exception as e:
                print("Bad message, skipping")
                print(e)
                continue
            if message is not None:
                print(f"Message received: {message}")
        except KeyboardInterrupt:
            break

    consumer.close()


# Main
if __name__ == '__main__':
    consumer_config, sr_config, topic = initialize_parser()
    consumer, avro_deserializer = initialize_consumer_deserializer(consumer_config, sr_config, topic)
    consume(consumer, avro_deserializer, topic)
