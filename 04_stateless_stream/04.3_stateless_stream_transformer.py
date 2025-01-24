from quixstreams import Application
import random
# Create an Application - the main configuration entry point
app = Application(
    broker_address="localhost:9093",
    consumer_group="transformer",
    auto_offset_reset="earliest",
)

# Define a topic with chat messages in JSON format
input_topic = app.topic(name="metrics", value_serializer="json")
output_topic = app.topic(name="metrics_transformed", value_serializer="json")



def produce_messages():
    with app.get_producer() as producer:
        for _ in range(10000):
            random_number = random.randint(1, 1000)
            message = {
                "metric_id": "id"+str(random_number),
                "number": random_number
            }
            # Serialize chat message to send it to Kafka
            # Use "chat_id" as a Kafka message key
            kafka_msg = input_topic.serialize(key=message["metric_id"], value=message)

            # Produce chat message to the topic
            print(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=input_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )
def transform():

    # Create StreamingDataFrame and connect it to the input topic
    sdf = app.dataframe(topic=input_topic)
    sdf = (
        sdf.apply(lambda value: {'number_multiplied': value['number'] * 5 })

        # Print the result to the console
        .update(print)
    )

    # Publish data to the output topic
    sdf = sdf.to_topic(output_topic)

    # Run the pipeline
    app.run()


if __name__ == "__main__":
    produce_messages()
    transform()