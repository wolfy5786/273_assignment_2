import uuid
import json
import os
import time

import pika
import structlog
from flask import Flask, jsonify, request

from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# In-memory order store
orders = {}


def ensure_topology():
    """Declare exchanges and queues needed by this service."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
    channel.queue_declare(
        queue="order_placed_queue",
        durable=True,
        arguments={"x-dead-letter-exchange": "dlx_exchange"},
    )
    channel.queue_bind(queue="order_placed_queue", exchange="orders", routing_key="order.placed")
    connection.close()
    log.info("topology_declared", service="order_service")


def publish_message(exchange, routing_key, message):
    """Open a new connection, publish one message, close."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/json",
        ),
    )
    connection.close()


@app.get("/health")
def health():
    log.info("health_check", service="order_service", status="running")
    return jsonify(status="ok")


@app.post("/order")
def place_order():
    start = time.time()

    data = request.get_json(force=True)
    item = data.get("item")
    quantity = data.get("quantity", 1)

    if not item:
        return jsonify(error="missing 'item' field"), 400

    order_id = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "item": item,
        "quantity": quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    orders[order_id] = order

    # Publish OrderPlaced event to RabbitMQ
    publish_message("orders", "order.placed", order)

    latency_ms = int((time.time() - start) * 1000)
    log.info(
        "order_placed",
        service="order_service",
        order_id=order_id,
        item=item,
        quantity=quantity,
        latency_ms=latency_ms,
    )

    return jsonify(order), 201


@app.get("/orders")
def get_orders():
    return jsonify(list(orders.values()))


if __name__ == "__main__":
    ensure_topology()
    log.info("starting", service="order_service", port=8080)
    app.run(host="0.0.0.0", port=8080)
