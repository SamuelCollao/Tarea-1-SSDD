FROM flink:latest

WORKDIR /app

RUN apt-get update && apt-get install -y pyhton3 python3-pip git
COPY requirements.txt .
RUN pip3 install pyflink kafka-python

COPY flink.py /app/