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

# In-memory notification log
notifications = []


def ensure_topology():
    """Declare exchanges and queues needed by this service."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.exchange_declare(exchange="inventory", exchange_type="topic", durable=True)
    channel.queue_declare(
        queue="inventory_reserved_queue",
        durable=True,
        arguments={"x-dead-letter-exchange": "dlx_exchange"},
    )
    channel.queue_bind(queue="inventory_reserved_queue", exchange="inventory", routing_key="inventory.reserved")
    connection.close()
    log.info("topology_declared", service="notification_service")


def on_inventory_event(ch, method, properties, body):
    """Callback for messages from inventory_reserved_queue."""
    try:
        message = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        log.error("malformed_json", service="notification_service", raw=body[:200] if body else "")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    order_id = message.get("order_id", "unknown")
    status = message.get("status", "unknown")

    notification = {
        "order_id": order_id,
        "status": status,
        "message": f"Order {order_id}: inventory {status}",
        "timestamp": time.time(),
    }
    notifications.append(notification)

    log.info(
        "notification_sent",
        service="notification_service",
        order_id=order_id,
        inventory_status=status,
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
                queue="inventory_reserved_queue",
                on_message_callback=on_inventory_event,
                auto_ack=False,
            )
            log.info("consumer_started", service="notification_service", queue="inventory_reserved_queue")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("rabbitmq_connection_lost", service="notification_service")
            time.sleep(5)
        except Exception:
            log.exception("consumer_error", service="notification_service")
            time.sleep(5)


@app.get("/health")
def health():
    log.info("health_check", service="notification_service", status="running")
    return jsonify(status="ok")


@app.get("/notifications")
def get_notifications():
    return jsonify(notifications)


if __name__ == "__main__":
    ensure_topology()

    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()

    log.info("starting", service="notification_service", port=8081)
    app.run(host="0.0.0.0", port=8081)
