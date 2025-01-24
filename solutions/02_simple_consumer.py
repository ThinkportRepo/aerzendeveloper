from confluent_kafka import Consumer

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
def main():
    kafka_topic_name = "my.first.topic"

    kafka_consumer = setup_kafka_consumer()
    kafka_consumer.subscribe([kafka_topic_name])

    latch = False
    while not latch:
        consumed_message = kafka_consumer.poll(5)
        if consumed_message is None:
            continue
        if consumed_message.error() is not None:
            latch = True
            continue
        print(
            f"Successfully consumed message: {consumed_message.key()} "
            f"from topic: {consumed_message.topic()}, "
            f"Value: {consumed_message.value().decode('utf-8')} "
            f"partition: [{consumed_message.partition()}] "
            f"at offset: {consumed_message.offset()}"
        )
    print("Error: " + consumed_message.error())
    kafka_consumer.close()

if __name__ == "__main__":
    main()
