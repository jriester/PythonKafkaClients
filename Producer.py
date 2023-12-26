import string
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import logging
import json


# Stats callback, add statistics.interval.ms to your client.ini for your client to have this trigger
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    with open("./Producer_stats.log", "w") as f:
        json.dump(stats_json, f)


def initialize_parser():
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['producer'])
    # Change below to what you're connecting to, client.ini is currently set up for:
    #     CP-Demo: mTLS connection : 'cp-demo'
    #     Support's CCloud Basic Cluster: 'basic'
    #     Support's CCLoud Standard Cluster: 'standard'
    config.update(config_parser['basic'])
    topic = dict(config_parser['topic'])

    return config, topic['topic']


# Create logger, initialize producer
def initialize_producer(config, topic):
    # Set up logger, keep this at DEBUG and use the producer config (debug:) to tune debug levels
    logger = logging.getLogger('producer')
    # Can change filename field below to an absolute path, otherwise writes to pwd
    logging.basicConfig(filename='producer.log', encoding='utf-8', level=logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    producer = Producer(config, logger=logger)

    return producer


# Simple callback on delivery
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        # print(f"Debug\nTopic: {msg.topic()} |  Partition: {msg.partition()} | Offset: {msg.offset()}")
        sys.stderr.write('%% Message %s delivered to %s [%d] %s\n ' %
                         (msg.value().decode('utf-8'), msg.topic(), msg.partition(), msg.offset()))


# Produce messages with no key with values taken from the console
def console_produce(producer):
    for line in sys.stdin:
        try:
            producer.produce(topic, line.rstrip(), callback=delivery_callback)
            producer.poll(0)
        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))


# Produce n random messages (one character) in key:value pairs
def data_produce(producer, topic):
    try:
        for _ in range(100):
            producer.produce(topic, choice(string.ascii_letters), choice(string.ascii_letters),
                             callback=delivery_callback)
            producer.poll()
    except:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))


# Produce a specific string
def specific_message(producer, topic):
    message = "my_message"
    try:
        producer.produce(topic, message,
                         callback=delivery_callback)
        # Poll for callback
        producer.poll()
    except:
        sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                         len(producer))


# General setup for transactions with multiple messages
def transaction_produce(producer, topic):
    producer.init_transactions()
    for i in range(1, 11):
        message1 = {
            "value": {
                "type": "trigger",
                "qn": f"first-{i}",
                "corr_id": f"1234-{i}"
            }
        }
        message2 = {
            "value": {
                "type": "trigger",
                "qn": f"second-{i}",
                "corr_id": f"1234-{i}"
            }
        }
        messages = [
            message1,
            message2
        ]

        producer.begin_transaction()
        for message in messages:
            key = message.get('key', "my_key")
            value = message.get('value', None)
            my_topic = message.get('topic', topic)
            producer.produce(topic=my_topic,
                             value=json.dumps(value),
                             key=key,
                             on_delivery=delivery_callback)

        producer.commit_transaction()


# Main
if __name__ == '__main__':
    config, topic = initialize_parser()
    producer = initialize_producer(config, topic)
    specific_message(producer, topic)

    producer.flush()
