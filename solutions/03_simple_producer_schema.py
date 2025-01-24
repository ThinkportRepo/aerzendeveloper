from confluent_kafka import Producer
import uuid
import json
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry.json_schema import JSONSerializer

# We might also want to access the Schema Registry
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    topic_subject_name_strategy,
)

def setup_kafka_producer():
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "acks": "all",
        "enable.idempotence": "true",
        "max.in.flight.requests.per.connection": "5"
    }
    return Producer(producer_config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def setup_schema_registry_client():
    return SchemaRegistryClient(
        {
            "url": "http://localhost:8081",
        }
    )


def get_subject_from_registry(kafka_topic_name, schema_registry_client):
    subject = "".join([kafka_topic_name, "-value"])
    return schema_registry_client.get_latest_version(subject)


def setup_string_serializer():
    return StringSerializer("utf_8")


def setup_json_serializer(schema_str, schema_registry_client):
    json_serializer_config = {
        "auto.register.schemas": True,
        "normalize.schemas": False,
        "use.latest.version": False,
        "subject.name.strategy": topic_subject_name_strategy,
    }
    return JSONSerializer(
        schema_str,
        schema_registry_client,
        conf=json_serializer_config,
    )
def main():
    kafka_producer = setup_kafka_producer()
    schema_registry_client = setup_schema_registry_client()
    kafka_topic_name = 'my.first.topic.with.schema'

    try:
        schema_str = get_subject_from_registry(kafka_topic_name, schema_registry_client).schema.schema_str
    except Exception as e:
        print("Exception: " + str(e))
    print(schema_str)
    json_serializer = setup_json_serializer(schema_str, schema_registry_client)
    string_serializer = setup_string_serializer()

    json_message = json.loads('{"myField1": 0,"myField2": 0.0,"myField3": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}')
    kafka_message_value = json_serializer(json_message
                                          ,SerializationContext(kafka_topic_name, MessageField.VALUE),
                                          )
    kafka_message_key = string_serializer(str(uuid.uuid4()))
    for i in range(0,2):
        kafka_producer.poll(0)
        kafka_producer.produce(
            topic=kafka_topic_name,
            key=kafka_message_key,
            value=kafka_message_value,
            on_delivery=delivery_report,
            headers={
                'a': 'A',
                'c': '{"a":"b"}',
                'b': 'C',
            }
        )
    kafka_producer.flush()

if __name__ == "__main__":
    main()