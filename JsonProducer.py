from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import json
import sys
import logging


# Stats callback, add statistics.interval.ms to your client.ini for your client to have this trigger
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    with open("./AvroConsumer_stats.log", "w") as f:
        json.dump(stats_json, f)


def initialize_parser():
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['producer'])
    # Change below to what you're connecting to, client.ini is currently set up for:
    #     CP-Demo: mTLS connection
    #     Support's CCloud Basic Cluster
    #     Support's CCLoud Standard Cluster
    config.update(config_parser['basic'])
    topic_name = dict(config_parser['topic'])
    # sr_config will either be schemaregistry for CCloud or cp-schemaregistry for CP-Demo
    sr_config = dict(config_parser['schemaregistry'])

    return config, sr_config, topic_name['topic']


def init_producer_serializer(config, sr_conf, topic):
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

    # Set up logger, keep this at DEBUG and use the producer config (debug:) to tune debug levels
    logger = logging.getLogger('producer')
    # Can change filename field below to an absolute path, otherwise writes to pwd
    logging.basicConfig(filename='JsonProducer.log', encoding='utf-8', level=logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Initialize producer, ignore highlighted logger below if IDE flags it as it's a false positive.
    producer = Producer(config, logger=logger)

    # JsonSerializer confs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/_modules/confluent_kafka/schema_registry/json_schema.html
    # ``auto.register.schemas`` | bool
    # ``normalize.schemas``     | bool
    # ``use.latest.version``    | bool
    # ``subject.name.strategy`` | callable
    serializer_conf = {'normalize.schemas': True, 'auto.register.schemas': False, 'use.latest.version': True}
    json_serializer = JSONSerializer(schema_str, schema_registry_client, conf=serializer_conf)

    # If using a key, use the below serializer
    # key_str = schema_registry_client.get_latest_version(f"{topic}-key").schema.schema_str
    # key_conf = {"auto.register.schemas": False}
    # key_serializer = JSONSerializer(key_str, schema_registry_client, conf=key_conf)
    # Or if you're using a String as a key
    key_serializer = StringSerializer('utf_8')

    return producer, json_serializer, key_serializer


# Simple callback to report topic-partition offset a message was produced to
def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record: {}".format(err))
        return
    print(f'Record successfully produced to {msg.topic()}-{msg.partition()} at offset {msg.offset()}')


# Synchronous producer which takes input like a console producer, does not support a key (use message_produce())
def console_produce_no_key(producer, json_serializer, topic):
    for line in sys.stdin:
        my_line = json.loads(line.rstrip())
        try:
            producer.produce(topic=topic,
                             value=json_serializer(json.loads(my_line),
                                                   SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

        except KeyboardInterrupt:
            exit()
        except ValueError as e:
            print("Invalid input, discarding record...")
            print(e)


# Produce a specific JSON message
def produce_specific_message(producer, json_serializer, topic, key_serializer):
    # to produce a message from a file, use below
    # with open('./json_message.json') as f:
    #    message = f.read()

    # Create your message a dictionary
    msg = {"amount": 500, "id": "10", "name": "james"}
    msg = json.dumps(msg)

    # Create your key as a dictionary
    key = {"my_key": 1}
    key = json.dumps(key)
    try:
        producer.produce(topic=topic,
                         # key=key_serializer(json.loads(key), SerializationContext(topic, MessageField.KEY)),
                         # If using a String as a key
                         # key=string_serializer(str(<Your string here>))
                         value=json_serializer(json.loads(msg),
                                               SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

    except KeyboardInterrupt:
        exit()
    except ValueError as e:
        print("Invalid input, discarding record...")
        print(e)
    # Poll for delivery callback
    producer.poll()


# Main
if __name__ == '__main__':
    producer_conf, sr_conf, topic = initialize_parser()
    producer, serializer, key_serializer = init_producer_serializer(producer_conf, sr_conf, topic)
    produce_specific_message(producer, serializer, topic, key_serializer)
    # console_produce_no_key(producer, serializer, topic)

    # Flush producer to ensure all in-flight messages are written
    producer.flush()
