import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
Topic_input = 'llm_retry_overload'
Topic_output = 'nueva_pregunta'
Topic_dlq = 'llm_dead_letter_queue'

BASE_RETRY_DELAY = int(os.getenv('OVERLOAD_BASE_DELAY', '5'))
MAX_RETRIES = int(os.getenv('OVERLOAD_MAX_RETRIES', '4'))

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
    producer= get_kafka_producer()
    consumer = get_kafka_consumer(Topic_input)

    print(f"Gestor de sobrecarga iniciado, Esperando fallos...")
    print(f"Retardo Exponencial Backoff activado: Base={BASE_RETRY_DELAY} s, Max Reintentos={MAX_RETRIES}")

    for message in consumer:
        data = message.value
        retry_contador = data.get('retry_count', 1)

        print(f"\nRecibido fallo de sobrecarga: {data['question_id']} - Intento {retry_contador} de {MAX_RETRIES}")

        if retry_contador > MAX_RETRIES:
            print(f"Máximo de reintentos alcanzado para {data['question_id']}. Enviando a DLQ ({Topic_dlq}).")
            producer.send(Topic_dlq, data)
        else:
            
            tiempo_espera = BASE_RETRY_DELAY * (2 ** (retry_contador - 1))
            print(f"Reintentando en {tiempo_espera} segundos...")
            time.sleep(tiempo_espera)

            producer.send(Topic_output, data)
            print(f"Reintento {retry_contador} para {data['question_id']}.")

        producer.flush()
        consumer.commit()

if __name__ == "__main__":
    main()