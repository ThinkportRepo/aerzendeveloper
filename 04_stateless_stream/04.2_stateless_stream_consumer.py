from quixstreams import Application

# Create an Application - the main configuration entry point
app = Application(
    broker_address="localhost:9093",
    consumer_group="text-splitter-v1",
    auto_offset_reset="earliest",
)

# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="messages", value_deserializer="json")

# Create a StreamingDataFrame - the stream processing pipeline
# with a Pandas-like interface on streaming data
sdf = app.dataframe(topic=messages_topic)
# Print the input data
sdf = sdf.update(lambda message: print(f"Input:  {message}"))

# Calculate the word length and store the result in the column
sdf["length"] = sdf["text"].apply(lambda word: len(word))

# Print the output result
sdf = sdf.update(lambda output: print(f"Wordcount: {output}"))



# Run the streaming application
if __name__ == "__main__":
    app.run()