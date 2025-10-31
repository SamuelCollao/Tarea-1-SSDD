import os 
import json
import random
import numpy as np
import torch
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from sentence_transformers import SentenceTransformer
from pyflink.common.watermark_strategy import WatermarkStrategy
from sklearn.metrics.pairwise import cosine_similarity

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
Topic_input = 'llm_success'
Topic_output_final = 'resultado_final'
Topic_output_regenerate = 'generar_flink'

SCORE_THRESHOLD = 0.8

MODEL_NAME = 'all-MiniLM-L6-v2'
MODEL = None
DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'

INPUT_SCHEMA = Types.ROW_NAMED(
['question_key', 'question_content', 'best_answer', 'llm_answer', 'question_title'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])

OUTPUT_SCHEMA = Types.ROW_NAMED(
    ['question_key', 'question_content', 'best_answer', 'llm_answer', 'question_title', 'score'],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT()])


def init_model():
    global MODEL
    if MODEL is None:
        print(f"Cargando modelo '{MODEL_NAME}' en {DEVICE}...")
    try:
        MODEL = SentenceTransformer(MODEL_NAME, device=DEVICE)
        print("Modelo cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el modelo: {e}")
        MODEL = None
    return MODEL

def calculate_quality_score(row):
    global MODEL
    model = init_model()

    if model is None:
        print("Modelo no disponible, asignando score 0.0")
        return row + (0.0,)

    llm_answer = row.llm_answer
    best_answer = row.best_answer
    score = 0.0

    if llm_answer and best_answer:
        try:
            embeddings = model.encode([llm_answer, best_answer], convert_to_tensor=True)
            emb_np = embeddings.cpu().numpy()
            similitud = cosine_similarity([emb_np[0]], [emb_np[1]])[0][0]
            score = max(0.0, min(1.0, similitud))

        except Exception as e:
            print(f"Error al calcular la similitud: {e}")
            score = 0.0

    print(f"Score calculado para la pregunta {row.question_key}: {score:.4f}")
    return row + (float(score),)

def filtrar(stream):

    score_stream = stream.map(
        calculate_quality_score,
        output_type=OUTPUT_SCHEMA
    )

    score_baja_calidad = score_stream \
        .filter(lambda row: row.score < SCORE_THRESHOLD)
    
    score_alta_calidad = score_stream \
        .filter(lambda row: row.score >= SCORE_THRESHOLD) \

    serializer_baja_calidad = KafkaRecordSerializationSchema.builder() \
        .set_topic(Topic_output_regenerate) \
        .set_value_serialization_schema(JsonRowSerializationSchema.builder().with_type_info(OUTPUT_SCHEMA).build()) \
        .build()

    sink_baja_calidad = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(serializer_baja_calidad) \
        .build()

    score_baja_calidad.sink_to(sink_baja_calidad)
    print(f"Preguntas de baja calidad enviadas a '{Topic_output_regenerate}'")

    serializer_alta_calidad = KafkaRecordSerializationSchema.builder() \
        .set_topic(Topic_output_final) \
        .set_value_serialization_schema(JsonRowSerializationSchema.builder().with_type_info(OUTPUT_SCHEMA).build()) \
        .build()
    
    sink_alta_calidad = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_record_serializer(serializer_alta_calidad) \
        .build()

    score_alta_calidad.sink_to(sink_alta_calidad)
    print(f"Preguntas de alta calidad enviadas a '{Topic_output_final}'")

def flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(6000)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(Topic_input) \
        .set_group_id("flink_consumer_group") \
        .set_value_only_deserializer(
            JsonRowDeserializationSchema.builder().type_info(INPUT_SCHEMA).build()
        ) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()
    
    data = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    filtrar(data)
    print(f"Flink job iniciado, escuchando en el tópico '{Topic_input}'...")
    env.execute("Flink LLM Answer Quality Scoring")

if __name__ == "__main__":
    flink_job()