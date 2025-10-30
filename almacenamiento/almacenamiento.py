import os
import psycopg2
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
Topic_input = 'resultado_final'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_db')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            print("Conexión exitosa a la base de datos")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Error de conexión a la base de datos: {e}")
            time.sleep(5)

def init_db(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS resultados (
                    id SERIAL PRIMARY KEY,
                    question_key TEXT UNIQUE NOT NULL,
                    question_content TEXT NOT NULL,
                    best_answer TEXT,
                    llm_answer TEXT,
                    score REAL,
                    veces_consultada INTEGER DEFAULT 1
                );
            """)
        conn.commit()
        print("Almacenamiento: Esquema de la tabla 'resultados' verificado y/o creado.")
    except psycopg2.Error as e:
        print(f"Almacenamiento: Error al inicializar el esquema: {e}")
        conn.rollback()
        raise

def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                Topic_input,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='almacenamiento_group',
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
    conn = connect_db()
    init_db(conn)
    consumer = get_kafka_consumer()

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
                        veces_consultada = resultados.veces_consultada + 1
                """,(
                    data['question_key'],
                    data.get('question_title'),
                    data.get('question_content'),
                    data.get('best_answer'),
                    data['llm_answer'],
                    data.get('score')
                ))

            conn.commit()
            print(f"[DB/SUCCESS] Pregunta {data['question_id']} almacenada en la base de datos.")
        
        except psycopg2.Error as e:
            print(f"[DB/ERROR] Error al almacenar la pregunta {data['question_id']}: {e}")
            conn.rollback()
        except Exception as e:
            print(f"[ERROR] Error inesperado al almacenar la pregunta {data['question_id']}: {e}")

if __name__ == "__main__":
    main()                            