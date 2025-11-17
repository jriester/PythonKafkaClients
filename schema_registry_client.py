from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka.schema_registry import SchemaRegistryClient
import avro.schema
import json

parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
sr_config = dict(config_parser['schemaregistry'])
schema_registry_client = SchemaRegistryClient(sr_config)

with open("scratch", 'r') as f:
    schema = f.read()

my_schema = avro.schema.parse(json.dumps(schema))
schema_registry_client.register_schema("james-normalize-tester", my_schema, normalize_schemas="true")
