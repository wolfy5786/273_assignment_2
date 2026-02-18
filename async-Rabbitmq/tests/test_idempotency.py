"""
Test Idempotency:
Publish the same order_id twice via pika and verify stock is decremented only once.
"""

import json
import time
import uuid

import pika
import requests

RABBITMQ_HOST = "localhost"
INVENTORY_URL = "http://localhost:8082"


def publish_order(order_id, item="widget-A", quantity=1):
    """Publish an order message directly to RabbitMQ."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    message = {
        "order_id": order_id,
        "item": item,
        "quantity": quantity,
        "status": "placed",
        "timestamp": time.time(),
    }
    channel.basic_publish(
        exchange="orders",
        routing_key="order.placed",
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
        ),
    )
    connection.close()


def test_idempotency():
    # Get initial inventory
    resp = requests.get(f"{INVENTORY_URL}/inventory")
    initial_stock = resp.json()["widget-A"]
    print(f"Initial stock for widget-A: {initial_stock}")

    # Publish the SAME order_id twice
    order_id = str(uuid.uuid4())
    print(f"Publishing order {order_id} twice...")
    publish_order(order_id, "widget-A", 1)
    publish_order(order_id, "widget-A", 1)

    # Wait for processing
    time.sleep(3)

    # Check inventory — stock should only be decremented once
    resp = requests.get(f"{INVENTORY_URL}/inventory")
    final_stock = resp.json()["widget-A"]
    print(f"Final stock for widget-A: {final_stock}")

    expected = initial_stock - 1
    assert final_stock == expected, (
        f"Idempotency FAILED: expected {expected}, got {final_stock}"
    )
    print("PASSED: Stock decremented only once (idempotency works)")

    # Verify order_id appears in processed list
    resp = requests.get(f"{INVENTORY_URL}/processed")
    processed = resp.json()
    assert order_id in processed, f"Order {order_id} not in processed list"
    print("PASSED: Order ID recorded in processed set")


if __name__ == "__main__":
    test_idempotency()
