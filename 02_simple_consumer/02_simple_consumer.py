from confluent_kafka import Consumer

def setup_kafka_consumer():
    return Consumer(
        {
            "bootstrap.servers": "",
            "group.id": "",
            "auto.offset.reset": "",
            "enable.auto.commit": ,
            "session.timeout.ms": ,
        }
    )
def main():
    kafka_topic_name = "my.first.topic"

    kafka_consumer =
    kafka_consumer.subscribe([kafka_topic_name])
    kafka_consumer.close()

if __name__ == "__main__":
    main()
