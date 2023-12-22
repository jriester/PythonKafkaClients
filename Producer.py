import string
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import time
import logging
import json


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['basic'])
    config.update(config_parser['producer'])

    topic = "james-test"

    logger = logging.getLogger('producer')
    logging.basicConfig(filename='producer.log', encoding='utf-8', level=logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    producer = Producer(config, logger=logger)
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).

    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            # print(f"Debug\nTopic: {msg.topic()} |  Partition: {msg.partition()} | Offset: {msg.offset()}")
            sys.stderr.write('%% Message %s delivered to %s [%d] %s\n ' %
                             (msg.value().decode('utf-8'), msg.topic(), msg.partition(), msg.offset()))

    # Produce messages with no key with values taken from the console
    def console_produce():
        for line in sys.stdin:
            try:
                # Produce line (without newline)
                producer.produce(topic, line.rstrip(), callback=delivery_callback)
                producer.poll(0)
            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(producer))

    # Produce n random messages (one character) in key:value pairs
    def data_produce():
        try:
            for _ in range(100):
                producer.produce(topic, choice(string.ascii_letters), choice(string.ascii_letters),
                                 callback=delivery_callback)
                producer.poll()
        except:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))


    def customer_produce():
        producer.init_transactions()
        delay_between_event_seconds = 60
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
            logging.info(f'Message send count: {i}')
            time.sleep(delay_between_event_seconds)

    data_produce()

    #start_time = time.time()
    #customer_poll()
    #end_time = time.time()
    #total_time = end_time - start_time
    #print(f"Total time: {total_time}")

