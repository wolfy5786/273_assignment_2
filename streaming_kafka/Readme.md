# Part C – Streaming with Kafka

## Architecture

```
                          ┌──────────────┐
  POST /order ──────────► │   Producer   │
  POST /orders/bulk       │ (Flask:7001) │
                          └──────┬───────┘
                                 │  produces
                                 ▼
                        ┌─────────────────┐
                        │  OrderEvents    │  (3 partitions, keyed on order_id)
                        │  Kafka topic    │
                        └───┬─────────┬───┘
                   consumes │         │ consumes
                            ▼         ▼
               ┌────────────────┐  ┌──────────────────┐
               │InventoryConsumer│  │AnalyticsConsumer │
               │   (group:      │  │  (group:         │
               │  inventory-grp)│  │  analytics-grp)  │
               └───────┬────────┘  │  Flask:7002      │
                       │ produces  │  GET /metrics     │
                       ▼           │  POST /replay     │
              ┌─────────────────┐  └─────────┬─────────┘
              │InventoryEvents  │            │ consumes
              │  Kafka topic    │────────────┘
              └─────────────────┘
```

Both topics have **3 partitions** and a **7-day retention**.
Kafka runs in **KRaft mode** (no Zookeeper required).

## Build & Run

```bash
cd streaming_kafka
docker-compose up --build
```

Wait ~30 s for Kafka to become healthy and for `init-topics` to create
`OrderEvents` and `InventoryEvents`.

| Service            | Port | Purpose                       |
|--------------------|------|-------------------------------|
| kafka              | 9092 / 9094 | Broker (internal / external) |
| producer-order     | 7001 | HTTP → Kafka producer         |
| inventory-consumer | –    | OrderEvents → InventoryEvents |
| analytics-consumer | 7002 | Both topics → live metrics    |

## API Quick Reference

```bash
# single order
curl -X POST http://localhost:7001/order \
  -H 'Content-Type: application/json' \
  -d '{"item":"pizza","qty":1}'

# bulk load – 10 000 events
curl -X POST http://localhost:7001/orders/bulk \
  -H 'Content-Type: application/json' \
  -d '{"n":10000,"item":"mixed"}'

# live metrics
curl http://localhost:7002/metrics | python3 -m json.tool

# replay (recompute from offset 0)
curl -X POST http://localhost:7002/replay | python3 -m json.tool

# producer throughput stats
curl http://localhost:7001/stats
```

## Run the Tests

```bash
pip install requests kafka-python
python tests/test_kafka.py
```

The test script:
1. **Produces 10 000 events** in 4 batches of 2 500 (item=mixed).
2. **Measures consumer lag** by restarting inventory-consumer with
   `THROTTLE_MS=100` then burst-producing 3 000 events.
3. **Demonstrates replay**: calls `POST /replay`, which creates a
   disposable consumer group, seeks every partition to offset 0,
   re-reads all events, and recomputes metrics from scratch.

A `kafka_metrics_report.txt` file is generated at the end.

## What to Submit

| Artifact | Location |
|----------|----------|
| Metrics report | `tests/kafka_metrics_report.txt` (auto-generated) |
| Replay evidence | Before/after table printed by the test and in the report |
| Consumer lag chart | Lag readings printed by test 2 |

