from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "words",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda x: x.decode("utf-8")
)


for msg in consumer:
    print(msg.value)
