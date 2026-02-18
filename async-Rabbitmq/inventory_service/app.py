import json
import os
import threading
import time

import pika
import structlog
from flask import Flask, jsonify

from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

# In-memory inventory stock
inventory = {
    "widget-A": 100,
    "widget-B": 50,
    "widget-C": 200,
}

# Idempotency: track processed order IDs
processed_order_ids = set()


def ensure_topology():
    """Declare all exchanges and queues needed by this service."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    # DLX exchange and queue
    channel.exchange_declare(exchange="dlx_exchange", exchange_type="fanout", durable=True)
    channel.queue_declare(queue="dead_letter_queue", durable=True)
    channel.queue_bind(queue="dead_letter_queue", exchange="dlx_exchange", routing_key="")
    # Orders exchange + queue (with DLX)
    channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
    channel.queue_declare(
        queue="order_placed_queue",
        durable=True,
        arguments={"x-dead-letter-exchange": "dlx_exchange"},
    )
    channel.queue_bind(queue="order_placed_queue", exchange="orders", routing_key="order.placed")
    # Inventory exchange + queue
    channel.exchange_declare(exchange="inventory", exchange_type="topic", durable=True)
    channel.queue_declare(
        queue="inventory_reserved_queue",
        durable=True,
        arguments={"x-dead-letter-exchange": "dlx_exchange"},
    )
    channel.queue_bind(queue="inventory_reserved_queue", exchange="inventory", routing_key="inventory.reserved")
    connection.close()
    log.info("topology_declared", service="inventory_service")


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
            delivery_mode=2,
            content_type="application/json",
        ),
    )
    connection.close()


def on_order_placed(ch, method, properties, body):
    """Callback for messages from order_placed_queue."""
    # Parse message — malformed JSON goes to DLQ
    try:
        message = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        log.error("malformed_json", service="inventory_service", raw=body[:200] if body else "")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    order_id = message.get("order_id")
    item = message.get("item")
    quantity = message.get("quantity")

    # Validate required fields — missing fields go to DLQ
    if not order_id or not item or quantity is None:
        log.error(
            "missing_fields",
            service="inventory_service",
            order_id=order_id,
            item=item,
            quantity=quantity,
        )
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    # Idempotency check
    if order_id in processed_order_ids:
        log.info(
            "duplicate_order_skipped",
            service="inventory_service",
            order_id=order_id,
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Reserve stock
    current_stock = inventory.get(item, 0)
    if current_stock >= quantity:
        inventory[item] = current_stock - quantity
        processed_order_ids.add(order_id)

        event = {
            "order_id": order_id,
            "item": item,
            "quantity": quantity,
            "status": "reserved",
            "remaining_stock": inventory[item],
            "timestamp": time.time(),
        }
        publish_message("inventory", "inventory.reserved", event)

        log.info(
            "inventory_reserved",
            service="inventory_service",
            order_id=order_id,
            item=item,
            quantity=quantity,
            remaining_stock=inventory[item],
        )
    else:
        processed_order_ids.add(order_id)

        event = {
            "order_id": order_id,
            "item": item,
            "quantity": quantity,
            "status": "failed",
            "reason": "insufficient_stock",
            "available_stock": current_stock,
            "timestamp": time.time(),
        }
        publish_message("inventory", "inventory.failed", event)

        log.warning(
            "inventory_failed",
            service="inventory_service",
            order_id=order_id,
            item=item,
            requested=quantity,
            available=current_stock,
        )

    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    """Run the RabbitMQ consumer in a background thread."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, heartbeat=600)
            )
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue="order_placed_queue",
                on_message_callback=on_order_placed,
                auto_ack=False,
            )
            log.info("consumer_started", service="inventory_service", queue="order_placed_queue")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("rabbitmq_connection_lost", service="inventory_service")
            time.sleep(5)
        except Exception:
            log.exception("consumer_error", service="inventory_service")
            time.sleep(5)


@app.get("/health")
def health():
    log.info("health_check", service="inventory_service", status="running")
    return jsonify(status="ok")


@app.get("/inventory")
def get_inventory():
    return jsonify(inventory)


@app.get("/processed")
def get_processed():
    return jsonify(list(processed_order_ids))


if __name__ == "__main__":
    ensure_topology()

    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    log.info("starting", service="inventory_service", port=8082)
    app.run(host="0.0.0.0", port=8082)
