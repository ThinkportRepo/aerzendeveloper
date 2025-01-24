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
def setup_schema_registry_client():
    return SchemaRegistryClient(
        {
            "url": "",
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
    kafka_topic_name = 'my.first.topic.with.schema'

    try:
        schema_str = get_subject_from_registry(kafka_topic_name, schema_registry_client).schema.schema_str
    except Exception as e:
        print("Exception: " + str(e))
    print(schema_str)
    json_serializer = setup_json_serializer(schema_str, schema_registry_client)
    string_serializer = setup_string_serializer()

    json_message = json.loads('')
    kafka_message_value = json_serializer(json_message
                                          ,SerializationContext(kafka_topic_name, MessageField.VALUE),
                                          )
    kafka_message_key = string_serializer(str(uuid.uuid4()))

if __name__ == "__main__":
    main()