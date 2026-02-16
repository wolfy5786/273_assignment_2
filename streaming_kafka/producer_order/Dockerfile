FROM python:3.11-slim

WORKDIR /app

# install the shared utils package (structlog logging)
COPY setup.py .
COPY utils/ utils/
RUN pip install --no-cache-dir . flask kafka-python structlog

# copy service code
COPY streaming_kafka/producer_order/app.py .

CMD ["python", "app.py"]
