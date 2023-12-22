# PythonKafkaClients

# Initial Setup
1. Fill out client.ini, adding your cluster and client details and setting any necessary configurations
2. If using [CP-Demo](https://github.com/confluentinc/cp-demo), make sure `Kafka1`, `Kafka2`, and `SchemaRegistry` are up using `docker ps`

# Calling the clients from the terminal

All clients are called with the same syntax:
```python
python3 <filename> client.ini
e.g python3 Producer.py client.ini
```

# Producing tips

1. If using a key, make sure you fill out the appropriate code regarding your keys
    1. Make sure `key_serializer` is correct in the function `init_producer_serializer`
    2. Uncomment the key sections of `producer.produce(...)` in the fuction `produce_specific_message`
3. If you're not using a key for any messages, no changes need to be made 

[^1]:  [Confluent's confluent-kafka-python examples](https://github.com/confluentinc/confluent-kafka-dotnet/tree/master/examples)[^1]
