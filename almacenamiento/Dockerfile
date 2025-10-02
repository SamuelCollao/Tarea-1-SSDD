FROM python:3.11-slim
WORKDIR /app
COPY requirement.txt .
RUN apt-get update && apt-get install -y curl libpq-dev gcc \
    && pip install --no-cache-dir -r requirement.txt \
    && apt-get remove -y libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*
COPY . .
CMD ["python", "almacenamiento.py"]