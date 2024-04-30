from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
import os



def dict_to_order(obj,ctx):
    return dict(obj)


def create_consumer(cr_config:dict):
    schema = "orders_v1.avsc"
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()
        
    sr_client = SchemaRegistryClient(
        {
            "url":cr_config["sr_url"],
            "basic.auth.user.info":f"{cr_config['sr_api_key']}:{cr_config['sr_secret_key']}"
        }
    )
    avro_deserializer = AvroDeserializer(
        sr_client,
        schema_str,
        dict_to_order
    )
    
    consumer = Consumer(
        {
            "bootstrap.servers":cr_config["bootstrap.servers"],
            "security.protocol":cr_config["security.protocol"],
            "sasl.mechanism":cr_config["sasl.mechanism"],
            "sasl.username":cr_config["sasl.username"],
            "sasl.password":cr_config["sasl.password"],
            "group.id":"orders_group_01"
        }
    )
    consumer.subscribe(["new_orders"])

    return consumer,avro_deserializer