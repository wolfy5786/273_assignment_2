FROM python:3.11-slim

WORKDIR /app

COPY setup.py .
COPY utils/ utils/
RUN pip install --no-cache-dir . flask kafka-python structlog

COPY streaming_kafka/analytics_consumer/app.py .

CMD ["python", "app.py"]
