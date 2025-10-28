import os
import psycopg2
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
Topic_input = 'resultado_final'

DB_HOST = os.getenv('DB_HOST', 'postgres_db')
DB_NAME = os.getenv('DB_NAME', 'yahoo_respuestas_db')      
DB_PASS = os.getenv('DB_PASS', 'SSDDcontraseña')
DB_USER = os.getenv('DB_USER', 'user_SSDD')


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
            score REAL,
            veces_consultada INTEGER DEFAULT 1,
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Base de datos inicializada")

def get_kafka_consumer(topic):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                group_id='almacenamiento_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest'
            )
            print("Conectado a Kafka como consumidor.")
            return consumer
        except NoBrokersAvailable:
            print("Kafka consumidor no disponible, reintentando en 5 segundos...")
            time.sleep(5)

def main():
    conn = connect_db()
    init_db(conn)
    consumer = get_kafka_consumer(Topic_input)

    print(f"Almacenamiento iniciado, esperando mensajes...")

    for message in consumer:
        data = message.value
        print(f"\nRecibido resultado para almacenar: {data['question_id']} (SCORE: {data.get('score', 'N/A')})")

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO resultados (question_key, question_title, question_content, best_answer, llm_answer, score)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (question_key) DO UPDATE SET
                        llm_answer = EXCLUDED.llm_answer,
                        score = EXCLUDED.score,
                """,(
                    data['question_key'],
                    data.get('question_title'),
                    data.get('question_content'),
                    data.get('best_answer'),
                    data['llm_answer'],
                    data.get('score')
                ))

            conn.commit()
            consumer.commit()
            print(f"[DB/SUCCESS] Pregunta {data['question_id']} almacenada en la base de datos.")
        
        except psycopg2.Error as e:
            print(f"[DB/ERROR] Error al almacenar la pregunta {data['question_id']}: {e}")
            conn.rollback()
        except Exception as e:
            print(f"[ERROR] Error inesperado al almacenar la pregunta {data['question_id']}: {e}")

if __name__ == "__main__":
    main()                            