import pika
import redis
import time
import requests
import os
import json

# Configuración de RabbitMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_cache')
ALMACENAMIENTO_HOST = os.getenv('ALMACENAMIENTO_HOST', 'almacenamiento_servicio')
ALMACENAR_URL = f"http://{ALMACENAMIENTO_HOST}:5000"
Q_input = 'Q_preguntas'
Q_miss = 'Q_cache_misses'



try:
    cache_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    cache_client.ping()
    print("Conexión exitosa a Redis")
except Exception:
    print("Error de conexión a Redis")
    exit(1)


def setup_connection():
    
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
            channel = connection.channel()
            channel.queue_declare(queue=Q_input, durable=True)
            channel.queue_declare(queue=Q_miss, durable=True)
            print("Conexión exitosa a RabbitMQ")
            return channel
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error de conexión a RabbitMQ: {e}")
            time.sleep(5)

def notify_storage_hit(question_key):
    try:
        response = requests.post(f"{ALMACENAR_URL}/update_hit", json={"question_key": question_key}, timeout=5)
        if response.status_code == 200:
            print(f"[HIT/UPDATE] Notificación Almacenamiento para clave: {question_key}")
        else:
            print(f"[HIT/ERROR] Fallo al actualizar Almacenamiento: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[HIT/ERROR] No se pudo conectar con el Almacenamiento: {e}")

def callback(ch, method, properties, body):
    try: 
        message = json.loads(body.decode())
        question_key = message.get('question_key')

        cached_data = cache_client.get(question_key)

        if cached_data:
            # CACHE HIT
            print(f"[HIT] Pregunta encontrada en cache: {question_key}.")
            notify_storage_hit(question_key)
    
        else:
            # CACHE MISS
            print(f"[MISS] Pregunta no encontrada en cache: {question_key}. Enviando a Score/Genrador.")
            ch.basic_publish(
                exchange='', 
                routing_key=Q_miss, 
                body=body, 
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

if __name__ == "__main__":
    try:
        channel = setup_connection()
        print('Esperando mensajes en {Q_input}. Para salir presione CTRL+C')
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=Q_input, 
            on_message_callback=callback,
        )   
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()