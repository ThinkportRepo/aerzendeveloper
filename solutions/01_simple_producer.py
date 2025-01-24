from confluent_kafka import Producer
import uuid

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

def main():
    kafka_topic_name = 'my.first.topic'

    kafka_message_key = str(uuid.uuid4())
    kafka_message_value = '{"myField1": 0,"myField2": 0.0,"myField3": "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}'.encode('utf-8')


    kafka_producer = setup_kafka_producer()

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