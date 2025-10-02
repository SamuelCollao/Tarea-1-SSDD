Plataforma Distribuida para Análisis de Preguntas y Respuestas

Este proyecto implementa una plataforma de microservicios distribuida para analizar y comparar respuestas humanas del histórico dataset de Yahoo! Respuestas con respuestas generadas por un Large Language Model (LLM) a través de la API de Gemini de Google.

El sistema está diseñado para ser escalable, resiliente y eficiente, utilizando un conjunto de tecnologías modernas para procesar datos de manera asíncrona, optimizar el rendimiento con una caché en memoria y persistir los resultados para su posterior análisis.

Arquitectura del Sistema
El sistema sigue una arquitectura de microservicios desacoplados que se comunican a través de un bus de mensajes (RabbitMQ).

Flujo de Datos Principal (Cache Miss):

Generador de Tráfico: Lee una pregunta del dataset y la publica en la cola Q_preguntas.

Servicio de Caché: Consume la pregunta. Al no encontrarla en Redis, la reenvía a la cola Q_cache_misses.

Generador de Score: Consume la pregunta, llama a la API de Gemini para obtener una respuesta, calcula un score de similitud semántica y publica el resultado completo en Q_preguntas_con_score.

Servicio de Almacenamiento: Consume el resultado final, lo persiste en la base de datos PostgreSQL y escribe la respuesta en la caché de Redis para futuras consultas.

Flujo Optimizado (Cache Hit):

Generador de Tráfico: Envía una pregunta ya procesada.

Servicio de Caché: Encuentra la respuesta en Redis, la descarta y notifica directamente al Servicio de Almacenamiento vía HTTP para que incremente su contador de consultas.

Stack Tecnológico
Contenedor: Docker y Docker Compose

Lenguaje: Python 3.11

Bus de Mensajes: RabbitMQ

Base de Datos: PostgreSQL

Caché en Memoria: Redis

LLM: Google Gemini API

Pre-requisitos
Para levantar este proyecto, solo necesitas tener instaladas las siguientes herramientas:

Docker

Docker Compose

espliegue y Ejecución
Sigue estos pasos para poner en funcionamiento todo el sistema.

1. Clonar el Repositorio
git clone [https://github.com/tu-usuario/Tarea-1-SSDD](https://github.com/SamuelCollao/Tarea-1-SSDD.git)
cd Tarea 1 SSDD

2. Levantar los Servicios
Usa Docker Compose para construir las imágenes y levantar todos los contenedores en segundo plano.

docker-compose up -d --build

El primer inicio puede tardar unos minutos mientras Docker descarga las imágenes base y el modelo de sentence-transformers.

Cómo Verificar el Funcionamiento
1. Verificar el Estado de los Contenedores
docker-compose ps

Todos los servicios deberían mostrar el estado Up y, los que tienen healthcheck, deberían indicar (healthy).

2. Monitorear los Logs en Tiempo Real
Puedes ver lo que cada servicio está haciendo en tiempo real.

# Ver los logs de un servicio específico
docker-compose logs -f generador_trafico

# Ver los logs del servicio de caché para observar los HITS y MISSES
docker-compose logs -f cache

# Ver todos los logs del sistema a la vez
docker-compose logs -f

3. Acceder a la Interfaz de RabbitMQ
Puedes monitorear las colas de mensajes y el flujo de datos visualmente.

URL: http://localhost:15672

Usuario: guest

Contraseña: guest

4. Consultar la Base de Datos Directamente
Puedes conectarte a PostgreSQL para ver los resultados guardados.

docker-compose exec postgres_db psql -U user_SSDD -d yahoo_respuestas_db

Una vez dentro, puedes ejecutar consultas SQL:

-- Ver las últimas 5 respuestas guardadas
SELECT id, question_key, bert_score_f1, veces_consultada FROM resultados ORDER BY id DESC LIMIT 5;

-- Ver el número total de registros
SELECT COUNT(*) FROM resultados;