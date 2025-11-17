import string
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from datetime import datetime
from random import choice
import confluent_kafka.admin
import confluent_kafka.schema_registry
from confluent_kafka import KafkaException, Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
import logging


def initialize_parser():
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)

    admin_config = dict(config_parser['admin'])
    admin_config.update(config_parser['all-in-one'])

    topic = dict(config_parser['topic'])

    return admin_config, topic


def initialize_admin(config):

    logger = logging.getLogger('AdminClient')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('./logs/admin.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    admin = AdminClient(config, logger=logger)
    return admin


def createTopic(admin: AdminClient):
    topic = confluent_kafka.admin.NewTopic("james-test", 1, 1)
    print("Trying to create AdminClient topic")

    fs = admin.create_topics([topic])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))

        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print("Topic exists, moving on")
            else:
                print("Failed to create topic {}: {}\nExiting...".format(topic, e))


def getOffsets(admin: AdminClient):
    tp = TopicPartition("input-topic", 0)
    os = OffsetSpec.earliest()
    print(admin.list_offsets(topic_partition_offsets={tp:os}))


if __name__ == '__main__':
    admin_config,  topic = initialize_parser()
    admin = initialize_admin(admin_config)
    getOffsets(admin)

