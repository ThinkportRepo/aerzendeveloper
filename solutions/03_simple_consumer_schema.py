from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

def setup_kafka_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "my-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        }
    )

def setup_schema_registry_client():
    return SchemaRegistryClient(
        {
            "url": "http://localhost:8081",
        }
    )

def get_subject_from_registry(kafka_topic_name, schema_registry_client):
    subject = "".join([kafka_topic_name, "-value"])
    return schema_registry_client.get_latest_version(subject)

def main():
    kafka_consumer = setup_kafka_consumer()
    schema_registry_client = setup_schema_registry_client()
    kafka_topic_name = 'my.first.topic.with.schema'
    kafka_consumer.subscribe([kafka_topic_name])
    try:
        schema_str = get_subject_from_registry(kafka_topic_name, schema_registry_client).schema.schema_str
    except Exception as e:
        print("Exception: " + str(e))
        return
    print(schema_str)
    json_deserializer = JSONDeserializer(schema_str)

    latch = False
    while not latch:
        consumed_message = kafka_consumer.poll(5)
        if consumed_message is None:
            continue
        if consumed_message.error() is not None:
            latch = True
            continue
        kafka_message_value = json_deserializer(
            consumed_message.value(),
            SerializationContext(consumed_message.topic(), MessageField.VALUE),
        )
        print(
            f"Successfully consumed message: {consumed_message.key().decode('utf-8')} "
            f"from topic: {consumed_message.topic()}, "
            f"Value: {kafka_message_value} "
            f"partition: [{consumed_message.partition()}] "
            f"at offset: {consumed_message.offset()}"
        )
    print("Error: " + consumed_message.error())
    kafka_consumer.close()


if __name__ == "__main__":
    main()