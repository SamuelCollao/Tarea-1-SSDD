FROM flink:1.18.1-java11

WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    python-is-python3

RUN curl -f -L -o /opt/flink/lib/flink-connector-kafka-3.1.0-1.18.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar

RUN curl -f -L -o /opt/flink/lib/kafka-clients-3.6.1.jar \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar

COPY requirements.txt .

RUN pip3 install --no-cache-dir --use-deprecated=legacy-resolver -r requirements.txt

COPY flink.py /app/