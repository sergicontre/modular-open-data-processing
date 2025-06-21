import json, random, time, os
from datetime import datetime
from kafka import KafkaProducer

topic = "iot-sensor"
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

city = "Madrid"

while True:
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "city": city,
        "temperature": round(random.uniform(15, 35), 2),
        "humidity": round(random.uniform(30, 90), 2)
    }
    producer.send(topic, payload)
    print("Enviado:", payload)
    time.sleep(1)