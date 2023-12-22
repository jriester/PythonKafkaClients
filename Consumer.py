import logging
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError


# Stats callback, add statistics.interval.ms to your client.ini for your client to have this trigger
def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    with open("./Consumer_stats.log", "w") as f:
        json.dump(stats_json, f)


# Parse the input client.ini file
def initialize_parser():
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['consumer'])
    # Change below to what you're connecting to, client.ini is currently set up for:
    #     CP-Demo: mTLS connection
    #     Support's CCloud Basic Cluster
    #     Support's CCLoud Standard Cluster
    config.update(config_parser['basic'])
    topic = dict(config_parser['topic'])

    return config, topic['topic']


# Create logger and initialize consumer
def initialize_consumer(config, topic):
    # Set up logger, keep this at DEBUG and use the producer config (debug:) to tune debug levels
    logger = logging.getLogger('consumer')
    # Can change filename field below to an absolute path, otherwise writes to pwd
    logging.basicConfig(filename='consumer.log', encoding='utf-8', level=logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    consumer = Consumer(config, logger=logger)

    return consumer


# Standard poll, subscribe to topic and consume messages, print to screen
def poll(consumer, topic):
    consumer.subscribe([topic])
    # Set up a callback to handle the '--reset' flag.

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
                continue
            else:
                print(msg.value())
        except KeyboardInterrupt:
            break


# Consumes a list of messages
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer
#     Parameters
#         num_messages (int) – The maximum number of messages to return (default: 1).
#         timeout (float) – The maximum time to block waiting for message, event or callback (default: infinite (-1)). (Seconds)
def consume(consumer, topic):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.consume(10, 1)

            if msg:
                for x in msg:
                    if x.error():
                        print("ERROR: ", x.error())
                    else:
                        print(x.value())
            else:
                print("Msg is empty")

    except KafkaError as e:
        print("Kafka Error: ", e)
        pass
    except ValueError as e:
        print("Value error: ", e)
        pass


# Main
if __name__ == '__main__':
    consumer_config, topic = initialize_parser()
    consumer = initialize_consumer(consumer_config, topic)
    poll(consumer)
    # consume(consumer)

    consumer.close()
