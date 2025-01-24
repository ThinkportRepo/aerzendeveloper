from confluent_kafka import Producer
import uuid

def setup_kafka_producer():
    producer_config = {
        "bootstrap.servers": "",
        "acks": "",
        "enable.idempotence": "",
        "max.in.flight.requests.per.connection": ""
    }
    return Producer(producer_config)

def main():
    kafka_topic_name = 'my.first.topic'

    kafka_message_key = ""
    kafka_message_value = ''


if __name__ == "__main__":
    main()