from fastapi import FastAPI, HTTPException, status
from confluent_kafka import Producer, Consumer, KafkaError
import threading
import json
import logging

logging.basicConfig(level=logging.INFO)
app = FastAPI()

# Конфиг Kafka
KAFKA_BROKER = "kafka:9092"
TOPICS = ["user_events", "payment_events", "movie_events"]

# Producer
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

# Consumer функция
def consume_topic(topic):
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': f'{topic}_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Consumer error: {msg.error()}")
                continue
            logging.info(f"Consumed from {topic}: {msg.value().decode('utf-8')}")
    finally:
        consumer.close()

# Запуск consumer потоков в фоне
for topic in TOPICS:
    t = threading.Thread(target=consume_topic, args=(topic,), daemon=True)
    t.start()

# API для продюсинга событий
@app.post("/api/events/{event_type}", status_code=status.HTTP_201_CREATED)
def create_event(event_type: str, payload: dict):
    if event_type not in ["user", "payment", "movie"]:
        raise HTTPException(status_code=400, detail="Invalid event type")
    topic = f"{event_type}_events"
    try:
        producer.produce(topic, json.dumps(payload).encode('utf-8'))
        producer.flush()
        logging.info(f"Produced event to {topic}: {payload}")
    except Exception as e:
        logging.error(f"Failed to produce event: {e}")
        raise HTTPException(status_code=500, detail="Failed to produce event")
    return {"status": "success", "topic": topic, "payload": payload}

# Health endpoint для Event сервиса (Ingress)
@app.get("/api/events/health")
def events_health():
    return {"status": True}

# Общий health check
@app.get("/health")
def general_health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8082)