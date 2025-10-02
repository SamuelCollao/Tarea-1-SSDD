import pandas as pd
import numpy as np
import time
import pika
import random
import os
import json

# Configuración de RabbitMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
queue_name = 'Q_preguntas'
dataset_path = '/app/data/test.csv'
lamba_poisson = 5
tasa_uniforme = 0.2

def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST, 
            heartbeat=600, 
            blocked_connection_timeout=300, 
        ))

        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        print("Conexión exitosa a RabbitMQ")
        return channel
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error de conexión a RabbitMQ: {e}")
        return None, None

def get_wait_time(distribution_type):
    if distribution_type == 'poisson':
        return np.random.exponential(1.0 / lamba_poisson)
    elif distribution_type == 'uniforme':
        return tasa_uniforme
    else:
        raise ValueError("Distribución no soportada")

def generate_traffic(df, channel,distribution_type):
    
    print(f"Generando tráfico con distribución {distribution_type}")

    while True:
        try:
            sample = df.sample(n=1).iloc[0]

            question_key = str(sample['id'])
        
            message_data = {
                'question_id': question_key,
                'question_content': str(sample['pregunta']),
                'best_answer': str(sample['respuesta']),
                'question_key': question_key
            }
        
            message = json.dumps(message_data)
        
            channel.basic_publish(
                exchange='', 
                routing_key=queue_name, 
                body=message, 
                properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
            )
        
            wait_time = get_wait_time(distribution_type)
            print(f"Mensaje enviado ID = {question_key} ... | Esperando {wait_time:.4f} segundos")
            time.sleep(wait_time)

        except pika.exceptions.AMQPConnectionError as e:
        
            print(f"Error de conexión a RabbitMQ: {e}")
            channel= connect_rabbitmq()
        
        except Exception as e:
        
            print(f"Error al enviar el mensaje: {e}")
            time.sleep(10)
    
if __name__ == "__main__":
    try:

        df = pd.read_csv(dataset_path, encoding='latin-1', on_bad_lines='skip', header=None)
        df = df.iloc[:, [0, 1, 2,3]]
        df.columns = ['id', 'pregunta', 'cuerpo', 'respuesta']
        df.dropna(inplace=True)
        df = df.sample(frac=1).reset_index(drop=True).head(10000)

        print(f"Dataset cargado con {len(df)} preguntas.")
    
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        exit(1)
    
    channel = connect_rabbitmq()
    distribution = 'poisson' 
    generate_traffic(df, channel, distribution)
    time.sleep(30)
