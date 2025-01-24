## Produce Messages
1. Produce one Message to my-first-topic
2. Verify that the message was produced in KafkaUI
3. Flush messages after producing. Poll results before sending. 
4. Produce five more messages to my-first-topic
5. Produce one more message to my-first-topic using any key
6. Produce five more messages to my-first-topic Partition 2
7. Implement a call back handler to the producer to print out the offset the message is sent to and test it.
8. Produce 10000 more messages to my-first-topic with headers

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