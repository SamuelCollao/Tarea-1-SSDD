FROM python:3.11-slim
WORKDIR /app
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt
COPY . .
CMD ["python","-u", "generador_score.py"]