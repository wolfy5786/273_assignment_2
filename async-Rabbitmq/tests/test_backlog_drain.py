"""
Test Backlog Drain:
1. Stop the inventory_service container
2. Publish 10 orders (they queue up in order_placed_queue)
3. Restart inventory_service
4. Verify all 10 orders are drained (queue becomes empty)
"""

import json
import subprocess
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


def get_queue_message_count(queue_name):
    """Get the number of messages in a queue via pika."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    queue = channel.queue_declare(queue=queue_name, passive=True)
    count = queue.method.message_count
    connection.close()
    return count


def test_backlog_drain():
    # Step 1: Stop inventory_service
    print("Stopping inventory_service...")
    subprocess.run(
        ["docker", "compose", "stop", "inventory_service"],
        cwd="/Users/mokshith/Desktop/273_assignment_2/async-Rabbitmq",
        check=True,
    )
    time.sleep(5)

    # Step 2: Publish 10 orders while inventory_service is down
    order_ids = []
    print("Publishing 10 orders while inventory_service is stopped...")
    for i in range(10):
        oid = str(uuid.uuid4())
        order_ids.append(oid)
        publish_order(oid, "widget-B", 1)
        print(f"  Published order {i+1}/10: {oid}")

    # Verify messages are queued
    queued = get_queue_message_count("order_placed_queue")
    print(f"Messages in order_placed_queue: {queued}")
    assert queued >= 10, f"Expected >= 10 queued messages, got {queued}"

    # Step 3: Restart inventory_service
    print("Restarting inventory_service...")
    subprocess.run(
        ["docker", "compose", "start", "inventory_service"],
        cwd="/Users/mokshith/Desktop/273_assignment_2/async-Rabbitmq",
        check=True,
    )

    # Step 4: Wait and poll for drain (up to 60s)
    print("Waiting for backlog to drain...")
    drained = False
    for i in range(12):
        time.sleep(5)
        remaining = get_queue_message_count("order_placed_queue")
        print(f"  [{(i+1)*5}s] Messages remaining in queue: {remaining}")
        if remaining == 0:
            drained = True
            break

    assert drained, "Backlog drain FAILED: queue not empty after 60s"
    print("PASSED: All 10 orders drained from queue after restart")

    # Verify all order_ids were processed
    resp = requests.get(f"{INVENTORY_URL}/processed")
    processed = resp.json()
    for oid in order_ids:
        assert oid in processed, f"Order {oid} was not processed"
    print("PASSED: All 10 order IDs found in processed set")


if __name__ == "__main__":
    test_backlog_drain()
