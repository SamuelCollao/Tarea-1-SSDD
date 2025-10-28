import os
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
Topic_input = 'llm_retry_quota'
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

def get_kafka_consumer(topic):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                group_id='retry_quota_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Conectado a Kafka como consumidor.")
            return consumer
        except NoBrokersAvailable:
            print("Kafka consumidor no disponible, reintentando en 5 segundos...")
            time.sleep(5)

def main():
    producer = get_kafka_producer()
    consumer = get_kafka_consumer(Topic_input)

    print(f"Quota de reintentos iniciada, escuchando en el tópico '{Topic_input}'...")
    print(f"Retardo fijado a {RETRY_DELAY} segundos.")

    for message in consumer:
        data = message.value
        print(f"\nRecibido fallo de cuota: {data['question_id']} - Reintentando en {RETRY_DELAY} segundos...")
        time.sleep(RETRY_DELAY)

        producer.send(Topic_output, data)
        producer.flush()
        consumer.commit()
        print(f"Reintento para {Topic_input}.")
    
if __name__ == "__main__":
    main()