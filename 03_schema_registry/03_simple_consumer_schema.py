from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

def setup_schema_registry_client():
    return SchemaRegistryClient(
        {
            "url": "",
            "basic.auth.user.info": ""
        }
    )

def get_subject_from_registry(kafka_topic_name, schema_registry_client):
    subject = "".join([kafka_topic_name, "-value"])
    return schema_registry_client.get_latest_version(subject)

def main():
    try:
        schema_str = get_subject_from_registry(kafka_topic_name, schema_registry_client).schema.schema_str
    except Exception as e:
        print("Exception: " + str(e))
        return
    print(schema_str)
    json_deserializer = JSONDeserializer(schema_str)

        kafka_message_value = json_deserializer(
            consumed_message.value(),
            SerializationContext(consumed_message.topic(), MessageField.VALUE),
        )


if __name__ == "__main__":
    main()