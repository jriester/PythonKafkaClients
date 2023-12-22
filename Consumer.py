import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition, KafkaError

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['basic'])
    config.update(config_parser['consumer'])
    consumer = Consumer(config)

    topic = config_parser['topic']
    topic_name = topic['topic']
    consumer.subscribe([topic_name])
    # Set up a callback to handle the '--reset' flag.

    def msg_consume():
        try:
            while True:
                msg = consumer.consume(10, 1)
                if msg:
                    print(f"Length: {len(msg)}")
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
        finally:
            # Leave group and commit final offsets
            consumer.close()

    def poll():
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = consumer.poll(1.0)
                if msg is None:
                    print("Waiting...")
                    continue
                else:
                    print(
                       msg.value())
            except KeyboardInterrupt:
                break

    poll()
