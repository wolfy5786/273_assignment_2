# Part C – Streaming with Apache Kafka

**CMPE 273 – Assignment 2**
**Author:** Krishna Panjiyar

---

## Metrics Report

```
====================================================================
  PART C – KAFKA STREAMING METRICS REPORT
  Generated : 2026-02-15 22:38:37
====================================================================

[1] 10 000 Event Production
  total_orders      : 10000
  total_reserved    : 9813
  total_failed      : 187
  failure_rate      : 1.87 %
  orders_per_minute : 249419.4

[2] Consumer Lag (throttled @ 100 ms/msg)
  peak lag    : 3000
  final lag   : 2936
  readings    : [3000, 3000, 3000, 2936, 2936, 2936, 2936, 2936, 2936, 2936, 2936, 2936] …

[3] Replay Consistency
  metric                           before      after
  ──────────────────────────── ────────── ──────────
  total_orders                      13000      13000
  total_reserved                    13212      13212
  total_failed                        235        235
  failure_rate_pct                   1.75       1.75

====================================================================
```

Also saved as `tests/kafka_metrics_report.txt`.

---

## Evidence of Replay (Before and After)

```
====================================================================
  TEST 3 ▸ Replay – Reset Offset & Recompute
====================================================================

  Settling 10 s …

  [1/3] Metrics BEFORE replay:
    total_orders                 = 13000
    total_reserved               = 13212
    total_failed                 = 235
    failure_rate_pct             = 1.75
    orders_per_minute            = 7829.3

  [2/3] POST /replay  (resetting offsets to beginning) …
    Replay finished in 11.0 s

  [3/3] Metrics AFTER replay:
    total_orders                 = 13000
    total_reserved               = 13212
    total_failed                 = 235
    failure_rate_pct             = 1.75
    orders_per_minute            = 7855.4

  ── Consistency check ──
  metric                           before      after   match
  ──────────────────────────── ────────── ────────── ───────
  total_orders                      13000      13000     YES
  total_reserved                    13212      13212     YES
  total_failed                        235        235     YES
  failure_rate_pct                   1.75       1.75     YES
```

All four core metrics match exactly. `orders_per_minute` differs by 0.3% because it is derived from event timestamps and the replay processing window is slightly different.

The replay works by creating a new consumer group, seeking all Kafka partitions to offset 0, and re-reading every event from the `OrderEvents` and `InventoryEvents` topics. Since Kafka's log is append-only and immutable, re-reading produces identical counts.
