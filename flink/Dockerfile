FROM flink:1.18.1-java11

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip wget build-essential libpq-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install \
    apache-flink==1.18.1 \
    requests \
    psycopg2-binary

WORKDIR /opt/flink/usrlib
COPY ./flink.py .
USER flink