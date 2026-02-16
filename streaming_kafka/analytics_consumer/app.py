"""
AnalyticsConsumer – Kafka Streaming (Part C)
Reads : OrderEvents + InventoryEvents
Computes:
    • orders per minute
    • failure rate (%)
    • per-item breakdown
Endpoints:
    GET  /metrics  → current computed metrics
    POST /replay   → reset offset to beginning, recompute everything
    GET  /health
"""

import os, time, json, threading
from collections import defaultdict
from flask import Flask, jsonify
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import structlog
from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
CONSUMER_GROUP  = os.getenv("CONSUMER_GROUP", "analytics-group")

# ── thread-safe metrics store ─────────────────────────────────
lock = threading.Lock()
metrics: dict = {}


def _blank_metrics() -> dict:
    return {
        "total_orders": 0,
        "total_reserved": 0,
        "total_failed": 0,
        "failure_rate_pct": 0.0,
        "orders_per_minute": 0.0,
        "inventory_events": 0,
        "first_ts": None,
        "last_ts": None,
        "items": defaultdict(lambda: {"orders": 0, "reserved": 0, "failed": 0}),
        "minute_buckets": defaultdict(int),
        "replay_count": 0,
        "last_replay_ts": None,
    }


def _init_metrics():
    global metrics
    with lock:
        old_replay = metrics.get("replay_count", 0)
        old_replay_ts = metrics.get("last_replay_ts")
        metrics = _blank_metrics()
        metrics["replay_count"] = old_replay
        metrics["last_replay_ts"] = old_replay_ts


_init_metrics()


def _connect_consumer(group_id: str) -> KafkaConsumer:
    for attempt in range(30):
        try:
            c = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=5000,
            )
            log.info("analytics_consumer_connected", group=group_id)
            return c
        except NoBrokersAvailable:
            log.info("kafka_waiting", attempt=attempt + 1)
            time.sleep(2)
    raise RuntimeError("Kafka unreachable")


# ── event processing (idempotent – just counters) ─────────────

def _process(event: dict, topic: str):
    etype = event.get("event_type", "")
    ts    = event.get("timestamp", time.time())
    item  = event.get("item", "unknown")

    with lock:
        if metrics["first_ts"] is None:
            metrics["first_ts"] = ts
        metrics["last_ts"] = ts

        if topic == "OrderEvents" and etype == "OrderPlaced":
            metrics["total_orders"] += 1
            metrics["items"][item]["orders"] += 1
            metrics["minute_buckets"][int(ts // 60)] += 1

        elif topic == "InventoryEvents":
            metrics["inventory_events"] += 1
            if etype == "InventoryReserved":
                metrics["total_reserved"] += 1
                metrics["items"][item]["reserved"] += 1
            elif etype == "InventoryFailed":
                metrics["total_failed"] += 1
                metrics["items"][item]["failed"] += 1

        # derived
        decided = metrics["total_reserved"] + metrics["total_failed"]
        if decided:
            metrics["failure_rate_pct"] = round(
                metrics["total_failed"] / decided * 100, 2)
        if metrics["first_ts"] and metrics["last_ts"]:
            span = (metrics["last_ts"] - metrics["first_ts"]) / 60.0
            if span > 0:
                metrics["orders_per_minute"] = round(
                    metrics["total_orders"] / span, 1)


# ── background consumer loop ─────────────────────────────────

def _consume_loop():
    consumer = _connect_consumer(CONSUMER_GROUP)
    consumer.subscribe(["OrderEvents", "InventoryEvents"])
    log.info("analytics_consuming", topics=["OrderEvents", "InventoryEvents"])

    while True:
        try:
            batch = consumer.poll(timeout_ms=1000)
            for tp, msgs in batch.items():
                for m in msgs:
                    _process(m.value, tp.topic)
        except Exception as exc:
            log.error("analytics_consumer_error", err=str(exc))
            time.sleep(1)


# ── replay ────────────────────────────────────────────────────

def _replay():
    """Create a throwaway consumer group, seek-to-beginning, re-read everything."""
    log.info("replay_started")
    _init_metrics()

    replay_group = f"analytics-replay-{int(time.time())}"
    consumer = _connect_consumer(replay_group)
    consumer.subscribe(["OrderEvents", "InventoryEvents"])

    # force partition assignment then seek
    consumer.poll(timeout_ms=5000)
    assigned = consumer.assignment()
    if assigned:
        consumer.seek_to_beginning(*assigned)
        log.info("replay_seeked", partitions=len(assigned))

    empty = 0
    while empty < 5:
        batch = consumer.poll(timeout_ms=2000)
        if not batch:
            empty += 1
            continue
        empty = 0
        for tp, msgs in batch.items():
            for m in msgs:
                _process(m.value, tp.topic)

    consumer.close()

    with lock:
        metrics["replay_count"] += 1
        metrics["last_replay_ts"] = time.time()
        log.info("replay_complete",
                 total_orders=metrics["total_orders"],
                 total_reserved=metrics["total_reserved"],
                 total_failed=metrics["total_failed"],
                 failure_rate=metrics["failure_rate_pct"])


# ── HTTP endpoints ────────────────────────────────────────────

@app.get("/health")
def health():
    return jsonify(status="ok")


@app.get("/metrics")
def get_metrics():
    with lock:
        items_dict = {k: dict(v) for k, v in metrics["items"].items()}
        return jsonify(
            total_orders=metrics["total_orders"],
            total_reserved=metrics["total_reserved"],
            total_failed=metrics["total_failed"],
            failure_rate_pct=metrics["failure_rate_pct"],
            orders_per_minute=metrics["orders_per_minute"],
            inventory_events_processed=metrics["inventory_events"],
            first_event_time=metrics["first_ts"],
            last_event_time=metrics["last_ts"],
            items_breakdown=items_dict,
            replay_count=metrics["replay_count"],
            last_replay_time=metrics["last_replay_ts"],
            minute_buckets_count=len(metrics["minute_buckets"]),
        )


@app.post("/replay")
def trigger_replay():
    """Reset consumer offset → recompute all metrics from the log."""
    t = threading.Thread(target=_replay, daemon=True)
    t.start()
    t.join(timeout=120)

    with lock:
        return jsonify(
            status="replay_complete",
            total_orders=metrics["total_orders"],
            total_reserved=metrics["total_reserved"],
            total_failed=metrics["total_failed"],
            failure_rate_pct=metrics["failure_rate_pct"],
            orders_per_minute=metrics["orders_per_minute"],
            replay_count=metrics["replay_count"],
        )


# ── main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=_consume_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=7002)
