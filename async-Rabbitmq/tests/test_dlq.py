"""
Test Dead Letter Queue:
Publish invalid JSON and a message with missing fields, then verify they
land in the dead_letter_queue.
"""

import json
import time

import pika

RABBITMQ_HOST = "localhost"


def publish_raw(exchange, routing_key, body):
    """Publish a raw message to RabbitMQ."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()


def get_dlq_message_count():
    """Get the number of messages in the dead_letter_queue."""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    channel = connection.channel()
    queue = channel.queue_declare(queue="dead_letter_queue", passive=True)
    count = queue.method.message_count
    connection.close()
    return count


def test_dlq():
    initial_count = get_dlq_message_count()
    print(f"Initial DLQ message count: {initial_count}")

    # 1. Publish invalid JSON
    print("Publishing invalid JSON...")
    publish_raw("orders", "order.placed", "this is not json {{{")

    # 2. Publish valid JSON but missing required fields
    print("Publishing message with missing fields...")
    publish_raw(
        "orders",
        "order.placed",
        json.dumps({"foo": "bar"}),  # no order_id, item, quantity
    )

    # Wait for inventory_service to nack both messages
    time.sleep(5)

    final_count = get_dlq_message_count()
    print(f"Final DLQ message count: {final_count}")

    new_messages = final_count - initial_count
    assert new_messages == 2, (
        f"DLQ FAILED: expected 2 new messages, got {new_messages}"
    )
    print("PASSED: Both malformed messages landed in dead_letter_queue")


if __name__ == "__main__":
    test_dlq()
