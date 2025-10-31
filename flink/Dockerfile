FROM flink:1.18

WORKDIR /app

RUN apt-get update && apt-get install -y python3 python3-pip git

COPY requirements.txt .

RUN pip3 install --no-cache-dir --use-deprecated=legacy-resolver -r requirements.txt

COPY flink.py /app/