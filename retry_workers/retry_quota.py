import os
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
Topic_input = 'llm_failure_quota'
Topic_output = 'nueva_pregunta'
RETRY_DELAY = int(os.getenv("QUOTA_RETRY_DELAY", "60"))

def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Conectado a Kafka como productor.")
            return producer
        except NoBrokersAvailable:
            print("Kafka productor no disponible, reintentando en 5 segundos...")
            time.sleep(5)

def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                Topic_input,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='retry_quota_group',
                api_version=(0,10,2),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            print(f"Consumidor de Kafka conectado a los topics: {Topic_input}")
            return consumer
        except NoBrokersAvailable as e:
            print(f"Kakfa consumidor no disponible: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

def main():
    producer = get_kafka_producer()
    consumer = get_kafka_consumer()

    print(f"Quota de reintentos iniciada, escuchando en el tópico '{Topic_input}'...")
    print(f"Retardo fijado a {RETRY_DELAY} segundos.")

    for message in consumer:
        data = message.value
        print(f"\nRecibido fallo de cuota: {data['question_key']} - Reintentando en {RETRY_DELAY} segundos...")
        time.sleep(RETRY_DELAY)

        producer.send(Topic_output, data)
        producer.flush()
        print(f"Reintento para {Topic_input}.")
    
if __name__ == "__main__":
    main()