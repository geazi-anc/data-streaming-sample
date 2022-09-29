import random
from time import sleep
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda x: x.encode("utf-8")
)


while True:
    words = [
        "spark",
        "kafka",
        "streaming",
        "python"
    ]

    word = random.choice(words)
    future = producer.send("words", value=word)

    print(future.get(timeout=60))

    sleep(random.randint(1, 6))
