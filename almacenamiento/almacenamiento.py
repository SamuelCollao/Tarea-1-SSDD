from flask import Flask, request, jsonify
import os
import psycopg2
import time
import json
import threading
import pika
import redis

DB_HOST = os.getenv('DB_HOST', 'postgres_db')
DB_NAME = os.getenv('DB_NAME', 'yahoo_respuestas_db')      
DB_PASS = os.getenv('DB_PASS', 'SSDDcontraseña')
DB_USER = os.getenv('DB_USER', 'user_SSDD')

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_cache')
Q_final = 'Q_preguntas_con_score'

app = Flask(__name__)

try:
    cache_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    cache_client.ping()
    print("Conexión exitosa a Redis")
except Exception:
    print("Error de conexión a Redis")
    exit(1)

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            print("Conexión exitosa a la base de datos")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Error de conexión a la base de datos: {e}")
            time.sleep(5)

def init_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resultados (
            id SERIAL PRIMARY KEY,
            question_key TEXT UNIQUE NOT NULL,
            question_content TEXT NOT NULL,
            best_answer TEXT,
            llm_answer TEXT,
            bert_score_f1 REAL,
            veces_consultada INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Base de datos inicializada")

@app.route('/health', methods=['GET'])
def status():
    return jsonify({"status": "ok"}), 200
    
@app.route('/update_hit', methods=['POST'])
def update_hit():
    
    data = request.get_json()
    question_key = data.get('question_key')

    if not question_key:
        return jsonify({"error": "Falta 'question_key' en la solicitud"}), 400

    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE resultados SEt veces_consultada = veces_consultada + 1 WHERE question_key = %s", 
            (question_key,)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "Update exitoso"}), 200
    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")
        return jsonify({"error": {e}}), 500
    
def rabbitmq_callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode())
        conn = connect_db()
        cursor = conn.cursor()

        cursor.execute(
            """ INSERT INTO resultados (question_key, question_content, best_answer, llm_answer, bert_score_f1, veces_consultada)
            VALUES (%s, %s, %s, %s, %s,1)
            ON CONFLICT (question_key) DO UPDATE SET
                veces_consultada = resultados.veces_consultada + 1; """,
            (
                data['question_key'],
                data['question_content'],
                data['best_answer'],
                data['llm_answer'],
                data['bert_score_f1'],  
            )
        )

        conn.commit()
        print(f"[DB] Resultado almacenado para la pregunta: {data['question_key']}")

        try:
            cache_client.set(data['question_key'], json.dumps(data))
            print(f"[CACHE/UPDATE] Resultado almacenado en cache para la pregunta: {data['question_key']}")
        except Exception as e:
            print(f"[CACHE/ERROR] No se pudo actualizar la cache: {e}")
            
        cursor.close()
        conn.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        print(f"Error al procesar el mensaje: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

def start_rabbitmq_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
            channel = connection.channel()
            channel.queue_declare(queue=Q_final, durable=True)
            print("Conexión exitosa a RabbitMQ")
            channel.basic_consume(
                queue=Q_final, 
                on_message_callback=rabbitmq_callback,
                auto_ack=False
            )   
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error de conexión a RabbitMQ: {e}")
            time.sleep(5)

if __name__ == '__main__':

    init_db()
    consumer_thread = threading.Thread(target=start_rabbitmq_consumer, daemon=True)
    consumer_thread.start()

    print("Iniciando servidor Flask en el puerto 5000")
    app.run(host='0.0.0.0', port=5000)