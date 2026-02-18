"""
Producer – Kafka Streaming (Part C)
POST /order         → produce single OrderPlaced to OrderEvents topic
POST /orders/bulk   → produce N orders at once  (load-testing)
GET  /stats         → producer throughput stats
GET  /health        → liveness
"""

import os, time, uuid, json, threading, random
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

import structlog
from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
ITEMS = ["burger", "pizza", "salad", "coffee", "sandwich",
         "taco", "sushi", "pasta", "wrap", "smoothie"]

# ── globals ───────────────────────────────────────────────────
producer = None
stats_lock = threading.Lock()
stats = {"produced": 0, "errors": 0, "start_time": time.time()}


def _connect_producer() -> KafkaProducer:
    """Retry-loop until Kafka is reachable."""
    global producer
    if producer is not None:
        return producer
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all",
                retries=3,
                linger_ms=5,
                batch_size=32768,
            )
            log.info("kafka_connected", bootstrap=KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.info("kafka_waiting", attempt=attempt + 1)
            time.sleep(2)
    raise RuntimeError("Cannot reach Kafka after 30 retries")


def _order_id() -> str:
    ts = format(int(time.time() * 1000), "x")[-8:]
    return f"ORD-{ts}-{uuid.uuid4().hex[:6]}"


def _produce_one(item: str, qty: int) -> tuple[str, dict]:
    oid = _order_id()
    event = {
        "event_type": "OrderPlaced",
        "order_id": oid,
        "item": item,
        "qty": qty,
        "timestamp": time.time(),
        "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    p = _connect_producer()
    future = p.send("OrderEvents", key=oid, value=event)
    future.add_errback(lambda e: log.error("produce_error", err=str(e)))
    with stats_lock:
        stats["produced"] += 1
    return oid, event


# ── HTTP endpoints ────────────────────────────────────────────

@app.get("/health")
def health():
    log.info("service=producer_order, endpoint=/health, status=running")
    return jsonify(status="ok")


@app.post("/order")
def create_order():
    body = request.get_json(force=True)
    item = body.get("item", "burger")
    qty  = body.get("qty", 1)
    oid, event = _produce_one(item, qty)
    log.info("order_produced", order_id=oid, item=item, qty=qty)
    return jsonify(order_id=oid, status="PRODUCED", event=event), 202


@app.post("/orders/bulk")
def bulk_orders():
    """Produce N orders.  item='mixed' rotates across all items."""
    body = request.get_json(force=True)
    n    = int(body.get("n", 100))
    item = body.get("item", "burger")

    start = time.time()
    ids = []
    for i in range(n):
        it = random.choice(ITEMS) if item == "mixed" else item
        oid, _ = _produce_one(it, 1)
        ids.append(oid)
    _connect_producer().flush()
    elapsed = time.time() - start

    log.info("bulk_produced", count=n, elapsed_s=round(elapsed, 3))
    return jsonify(
        produced=n,
        elapsed_seconds=round(elapsed, 3),
        rate_per_second=round(n / max(elapsed, 0.001), 1),
        first_order=ids[0],
        last_order=ids[-1],
    ), 202


@app.get("/stats")
def get_stats():
    with stats_lock:
        up = time.time() - stats["start_time"]
        return jsonify(
            produced=stats["produced"],
            errors=stats["errors"],
            uptime_seconds=round(up, 1),
            rate_per_second=round(stats["produced"] / max(up, 0.001), 1),
        )


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        _connect_producer()
    except Exception:
        log.warning("producer_deferred_connect")
    app.run(host="0.0.0.0", port=7001)
