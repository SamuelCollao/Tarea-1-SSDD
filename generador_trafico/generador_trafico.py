import pandas as pd
import json 
import time
import os
import psycopg2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
Topic_output = 'nueva_pregunta'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_db')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASS = os.getenv('POSTGRES_PASSWORD')
dataset_path = './data/test.csv'

Nombres_columnas = ['question_number','question_title', 'question_content', 'best_answer']

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

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASS
            )
            print("Conexión exitosa a la base de datos")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Error de conexión a la base de datos: {e}")
            time.sleep(5)
    
def check_question(cursor, key):
    try:
        cursor.execute("SELECT 1 FROM resultados WHERE question_key = %s", (key,))
        return cursor.fetchone() is not None
    except psycopg2.Error as e:
        print(f"Error al verificar la pregunta: {e}")
        return True
    
def main():
    producer = get_kafka_producer()
    conn = connect_db()

    try:
        df = pd.read_csv(dataset_path, encoding='latin-1', on_bad_lines='skip', header=None, names=Nombres_columnas).sample(frac=1).reset_index(drop=True)
        df = df.head(10000)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        exit(1)

    with conn.cursor() as cur:
        for index, row in df.iterrows():
            
            question_key = str(hash(str(row['question_title']) + str(row['question_content'])))

            if not check_question(cur, question_key):
                message = {
                    'question_key': question_key,
                    'question_title': row['question_title'],
                    'question_content': row['question_content'],
                    'best_answer': row['best_answer']
                }
                producer.send(Topic_output, message)
                print(f"Pregunta enviada: {question_key}")
                time.sleep(1)
            else:
                print(f"Pregunta duplicada: {question_key}")

    producer.flush()
    conn.close()
    print("Generación de tráfico completada.")

if __name__ == "__main__":
    main()
