from fastapi import FastAPI,status
from confluent_kafka import Producer, Consumer, KafkaError
import threading
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
app = FastAPI()

KAFKA_BROKER = "kafka:9092"
TOPICS = ["user_events", "payment_events", "movie_events"]

# Producer
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

# Consumer function
def consume_topic(topic):
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'{topic}_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logging.error(f"Consumer error: {msg.error()}")
            continue
        logging.info(f"Consumed from {topic}: {msg.value().decode('utf-8')}")
    consumer.close()

# Start consumers in background threads
for topic in TOPICS:
    t = threading.Thread(target=consume_topic, args=(topic,), daemon=True)
    t.start()





# API endpoints to produce events
@app.post("/api/events/{event_type}", status_code=status.HTTP_201_CREATED)
def create_event(event_type: str, payload: dict):
    if event_type not in ["user", "payment", "movie"]:
        return {"error": "Invalid event type"}
    topic = f"{event_type}_events"
    producer.produce(topic, json.dumps(payload).encode('utf-8'))
    producer.flush()
    logging.info(f"Produced event to {topic}: {payload}")
    return {"status": "success", "topic": topic, "payload": payload}

@app.get("/api/events/health")
def health():
    return {"ok": True}