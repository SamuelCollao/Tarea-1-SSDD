import pika
import requests
import json
import os
from bert_score import score
import torch
from sentence_transformers import SentenceTransformer, util

#Configuraciones de entorno

#RABBITMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
Q_miss = 'Q_cache_misses'
Q_Final = 'Q_preguntas_con_score'

# GEMINI
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GEMINI_MODEL = os.getenv('GEMINI_MODEL')
GEMINI_API_URL = os.getenv('GEMINI_API_URL')
GEMINI_ENDPOINT = f"{GEMINI_API_URL}/{GEMINI_MODEL}:generateContent"

# BERT-SCORE
DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'

def connect_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600))
        channel = connection.channel()
        channel.queue_declare(queue=Q_miss, durable=True)
        channel.queue_declare(queue=Q_Final, durable=True)
        print("Conexión exitosa a RabbitMQ")
        return connection, channel
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error de conexión a RabbitMQ: {e}")
        return None, None
    
def generate_llm_answer(question_content):
    
    if not GEMINI_API_KEY:
        print("[LLM/Error] La clave de API de Gemini no está configurada.")
        return None
    
    payload = {
        "contents": [{
            "parts": [{
                "text": f"Responde esta pregunta de manera concisa: {question_content}"
            }]
        }],
    }

    headers = {
        'Content-Type': 'application/json'
    }
    
    full_url = f"{GEMINI_API_URL}/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    
    try:
 
        response = requests.post(full_url, json=payload, headers=headers, timeout=300)

        response.raise_for_status()
        data = response.json()

        generated_text = data['candidates'][0]['content']['parts'][0]['text']
        return generated_text.strip()
    except requests.exceptions.RequestException as e:
        print(f"[LLM/Error] Error al llamar a la API de Gemini: {e}")
        return None
    except KeyError as e:
        print(f"[LLM/Error] Respuesta inesperada de la API de Gemini: {e}")
        return None
    
def calculate_bert_score(llm_asnwer, original_answer):
    model = SentenceTransformer('all-MiniLM-L6-v2', device = DEVICE)
    embed1 = model.encode([llm_asnwer], convert_to_tensor=True, device=DEVICE)
    embed2 = model.encode([original_answer], convert_to_tensor=True, device=DEVICE)
    score = util.pytorch_cos_sim(embed1, embed2).item()
    return score

def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        question_key = message['question_key']

        llm_answer = generate_llm_answer(message['question_content'])

        if llm_answer is None:
            raise Exception("Error al generar respuesta LLM")
        
        score= calculate_bert_score(message['best_answer'], llm_answer)

        final_result = {
            'question_key': question_key,
            'question_content': message['question_content'],
            'best_answer': message['best_answer'],
            'llm_answer': llm_answer,
            'bert_score_f1': score
        }

        ch.basic_publish(
            exchange='',
            routing_key = Q_Final,
            body = json.dumps(final_result),
            properties = pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE)
        )

        print(f"[SCORE] Pregunta procesada y enviada a {Q_Final}: {question_key} con BERT-Score: {score:.4f}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"[SCORE/ERROR] Error al procesar el mensaje: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

if __name__ == "__main__":
    connection, channel = connect_rabbitmq()
    print('Esperando mensajes. Para salir presione CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=Q_miss, 
        on_message_callback=callback,
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()
        connection.close()