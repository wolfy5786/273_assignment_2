"""
InventoryConsumer – Kafka Streaming (Part C)
Reads  : OrderEvents   (event_type = OrderPlaced)
Writes : InventoryEvents (InventoryReserved | InventoryFailed)

~2 % random failure rate so analytics has real failure data.
Set THROTTLE_MS > 0 to artificially slow the consumer (lag demo).
"""

import os, time, json, random
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

import structlog
from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
CONSUMER_GROUP  = os.getenv("CONSUMER_GROUP", "inventory-group")
THROTTLE_MS     = int(os.getenv("THROTTLE_MS", "0"))

# in-memory stock
inventory = {item: 50_000 for item in
             ["burger", "pizza", "salad", "coffee", "sandwich",
              "taco", "sushi", "pasta", "wrap", "smoothie"]}

stats = {"processed": 0, "reserved": 0, "failed": 0, "start": time.time()}


def _connect(cls, **kw):
    for attempt in range(30):
        try:
            return cls(**kw)
        except NoBrokersAvailable:
            log.info("kafka_waiting", role=cls.__name__, attempt=attempt + 1)
            time.sleep(2)
    raise RuntimeError("Kafka unreachable")


def main():
    consumer = _connect(
        KafkaConsumer,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        value_deserializer=lambda m: json.loads(m.decode()),
        max_poll_interval_ms=300_000,
    )
    consumer.subscribe(["OrderEvents"])

    out = _connect(
        KafkaProducer,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
    )

    log.info("inventory_consumer_started",
             group=CONSUMER_GROUP, throttle_ms=THROTTLE_MS,
             inventory_snapshot=inventory)

    for msg in consumer:
        event = msg.value
        if event.get("event_type") != "OrderPlaced":
            continue

        oid  = event.get("order_id", "?")
        item = event.get("item", "?")
        qty  = event.get("qty", 1)

        if THROTTLE_MS > 0:
            time.sleep(THROTTLE_MS / 1000.0)

        # ~2 % random failure
        if random.random() < 0.02:
            out.send("InventoryEvents", key=oid, value={
                "event_type": "InventoryFailed",
                "order_id": oid, "item": item,
                "reason": "random_stock_check_failure",
                "timestamp": time.time(),
                "src_offset": msg.offset, "src_partition": msg.partition,
            })
            stats["failed"] += 1
        elif item in inventory and inventory[item] >= qty:
            inventory[item] -= qty
            out.send("InventoryEvents", key=oid, value={
                "event_type": "InventoryReserved",
                "order_id": oid, "item": item, "qty": qty,
                "remaining": inventory[item],
                "timestamp": time.time(),
                "src_offset": msg.offset, "src_partition": msg.partition,
            })
            stats["reserved"] += 1
        else:
            out.send("InventoryEvents", key=oid, value={
                "event_type": "InventoryFailed",
                "order_id": oid, "item": item,
                "reason": f"insufficient_stock ({inventory.get(item, 0)})",
                "timestamp": time.time(),
                "src_offset": msg.offset, "src_partition": msg.partition,
            })
            stats["failed"] += 1

        stats["processed"] += 1
        if stats["processed"] % 1000 == 0:
            elapsed = time.time() - stats["start"]
            log.info("inventory_progress",
                     processed=stats["processed"],
                     reserved=stats["reserved"],
                     failed=stats["failed"],
                     rate=round(stats["processed"] / max(elapsed, 0.001), 0))


if __name__ == "__main__":
    main()
