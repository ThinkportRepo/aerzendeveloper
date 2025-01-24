## Consume Messages
1. Consume one message from my-first-topic and print out the Offset, Key, Value, Headers
```python
consumed_message = kafka_consumer.poll(1)
print(consumed_message.key())
```
2. Consume all messages from my-first-topic
```python
        if consumed_message is None:
            continue
        if consumed_message.error() is not None:
            print("ERROR")
            continue
```
3. Create a continuous Producer that publishes a random number between 1 and 10 every second in the topic "numbers"
4. Consume the numbers topic and print out the numbers
5. produce every number that is greater than 5 to a new "numbers.greater.five" topic 
6. create a second instance of the consumer with the same group id
7. View the assigned partitions of each instance

```python
kafka_producer.poll(0)
kafka_producer.produce(
            topic="",
            value=""
        )
kafka_producer.flush()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        "PrintError"
    else:
        """ Print msg.topic(), msg.partition() """
```