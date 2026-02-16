FROM python:3.11-slim

WORKDIR /app

COPY setup.py .
COPY utils/ utils/
RUN pip install --no-cache-dir . kafka-python structlog

COPY streaming_kafka/inventory_consumer/app.py .

CMD ["python", "app.py"]
