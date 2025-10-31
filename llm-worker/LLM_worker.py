import os
import json
import requests
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = os.getenv('GEMINI_MODEL')
GEMINI_API_URL = os.getenv('GEMINI_API_URL')

Topic_input = ['nueva_pregunta','generar_flink']
Topic_success = 'llm_success'
Topic_failure_quota = 'llm_failure_quota'
Topic_failure_overload = 'llm_failure_overload'

def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10),
                acks='all',
                retries=5
            )
            print("Conexión exitosa al productor de Kafka")
            return producer
        except NoBrokersAvailable as e:
            print(f"Kakfa productor no disponible: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                *Topic_input,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='llm_worker_group',
                api_version=(0,10,2),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            print(f"Consumidor de Kafka conectado a los topics: {Topic_input}")
            return consumer
        except NoBrokersAvailable as e:
            print(f"Kakfa consumidor no disponible: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

def call_gemini_api(question_title):
    if not GEMINI_API_KEY:
        print("[LLM/Error] La clave de API de Gemini no está configurada.")
        return None, 500

    headers = {
        'Content-Type': 'application/json'
    }
    payload = {
        "contents": [{
            "rol": "user",
            "parts": [{
                "text": f"Genera una respuesta detallada y precisa a la siguiente pregunta: {question_title}"}]}]
    }
  
    try:
        response = requests.post(f"{GEMINI_API_URL}?key={GEMINI_API_KEY}", json=payload, headers=headers, timeout=300)

        # Manejo de estados

        if response.status_code == 200:
            return 200, response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
        # Límite de cuota alcanzado
        elif response.status_code == 429:
            return 429, {"[LLM/Warning]": "Límite de cuota alcanzado"}
        # Sobrecarga del servicio
        elif response.status_code >= 500:
            return 503, {"[LLM/Warning]": f"Sobrecarga del servicio ({response.status_code}) detectado"}
        # Otros errores
        else:
            return response.status_code, {"[LLM/Error]": f"Error inesperado de la API de Gemini: {response.status_code} {response.text}"}
    except requests.exceptions.RequestException as e:
        print(f"[LLM/Error] Error de conexion: {e}")
        return 503, None

def main():
    producer = get_kafka_producer()
    consumer = get_kafka_consumer()

    print("LLM Worker iniciado y escuchando mensajes...")

    for message in consumer:
        data = message.value
        print(f"\nProcesando mensaje de {message.topic}: {data['question_key']}")

        status_code, response = call_gemini_api(data['question_title'])

        # Exito del mensaje ("200 OK")
        if status_code == 200 and response:
            output_message = {
                'question_key': data['question_key'],
                'question_content': data.get('question_content', None),
                'best_answer': data.get('best_answer', None),
                'llm_answer': response,
                'question_title': data['question_title']
            }
            producer.send(Topic_success, output_message)
            print(f"[LLM/Success] Mensaje procesado correctamente. Enviado a {Topic_success}")

        # Límite de cuota alcanzado ("429 Too Many Requests")
        elif status_code == 429:
            # Reintentar más tarde
            producer.send(Topic_failure_quota, data)
            print(f"[LLM/Warning] Límite de cuota alcanzado. Mensaje reenviado a {Topic_failure_quota}")
        
        # Sobrecarga del servicio ("503 Service Unavailable")
        elif status_code == 503:
            # Reintentos exponencial backoff
            data['retry_count'] = data.get('retry_count', 0) + 1
            producer.send(Topic_failure_overload, data)
            print(f"[LLM/Warning] Sobrecarga del servicio. Mensaje reenviado a {Topic_failure_overload}. (Reintento #{data['retry_count']})")
        
        # Otros errores
        else:
            print(f"[LLM/Error] Error {status_code} no manejado. Mensaje descartado.")

        producer.flush()


if __name__ == "__main__":

    if not GEMINI_API_KEY:
        print("[LLM/Error] La clave de API de Gemini no está configurada. Saliendo...")

    main()       