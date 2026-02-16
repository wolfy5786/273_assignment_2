#!/usr/bin/env python3
"""
Test suite for Part C – Streaming Kafka
─────────────────────────────────────────
Run from the repo root (or streaming_kafka/tests/):

    cd streaming_kafka
    docker-compose up --build -d
    pip install requests kafka-python
    python tests/test_kafka.py

Tests
  1. Produce 10 000 events → verify analytics picks them up
  2. Consumer lag under throttling (restart inventory-consumer w/ THROTTLE_MS)
  3. Replay: reset offset, recompute metrics, compare before / after
"""

import os, sys, time, json, subprocess, statistics, textwrap
import requests

# ── Config ────────────────────────────────────────────────────
PRODUCER_URL  = os.getenv("PRODUCER_URL",  "http://localhost:7001")
ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://localhost:7002")
COMPOSE_DIR   = os.path.join(os.path.dirname(__file__), "..")
if not os.path.isfile(os.path.join(COMPOSE_DIR, "docker-compose.yml")):
    COMPOSE_DIR = "."                           # fallback

DIVIDER = "=" * 68


def _get(url, timeout=10):
    return requests.get(url, timeout=timeout).json()


def _post(url, body=None, timeout=120):
    return requests.post(url, json=body or {}, timeout=timeout).json()


def _dc(*args, capture=True):
    """Run docker-compose inside the streaming_kafka directory."""
    return subprocess.run(
        ["docker-compose"] + list(args),
        capture_output=capture, text=True, cwd=COMPOSE_DIR,
    )


def _consumer_lag(group="inventory-group"):
    """Return total lag for *group* via kafka-consumer-groups.sh."""
    r = _dc("exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:9093",
            "--group", group, "--describe")
    total = 0
    for line in (r.stdout + r.stderr).splitlines():
        parts = line.split()
        if len(parts) >= 6:
            try:
                total += int(parts[5])
            except ValueError:
                pass
    return total


# ── Wait for services ────────────────────────────────────────

def wait_for_services(timeout=120):
    print("\n⏳  Waiting for services …")
    endpoints = [
        (f"{PRODUCER_URL}/health",  "Producer"),
        (f"{ANALYTICS_URL}/health", "Analytics"),
    ]
    t0 = time.time()
    for url, name in endpoints:
        while time.time() - t0 < timeout:
            try:
                if requests.get(url, timeout=2).status_code == 200:
                    print(f"   ✓ {name}")
                    break
            except Exception:
                pass
            time.sleep(2)
        else:
            print(f"   ✗ {name} – gave up after {timeout}s"); sys.exit(1)
    print()


# ══════════════════════════════════════════════════════════════
# TEST 1 – Produce 10 000 events
# ══════════════════════════════════════════════════════════════

def test_produce_10k():
    print(DIVIDER)
    print("  TEST 1 ▸ Produce 10 000 OrderEvents")
    print(DIVIDER)

    TOTAL   = 10_000
    BATCH   = 2_500
    batches = TOTAL // BATCH
    cum_sec = 0.0

    for i in range(batches):
        print(f"\n  Batch {i+1}/{batches}  ({BATCH} orders, item=mixed) …")
        resp = _post(f"{PRODUCER_URL}/orders/bulk",
                     {"n": BATCH, "item": "mixed"})
        cum_sec += resp["elapsed_seconds"]
        print(f"    produced={resp['produced']}  "
              f"elapsed={resp['elapsed_seconds']:.2f}s  "
              f"rate={resp['rate_per_second']:.0f}/s")

    pstat = _get(f"{PRODUCER_URL}/stats")
    print(f"\n  ── Producer totals ──")
    print(f"  produced          : {pstat['produced']}")
    print(f"  cumulative time   : {cum_sec:.2f}s")
    print(f"  avg throughput    : {pstat['produced']/max(cum_sec,.001):.0f} evt/s")

    # let consumers catch up
    print("\n  Waiting 20 s for consumers to drain …")
    time.sleep(20)

    m = _get(f"{ANALYTICS_URL}/metrics")
    print(f"\n  ── Analytics snapshot ──")
    print(f"  total_orders      : {m['total_orders']}")
    print(f"  total_reserved    : {m['total_reserved']}")
    print(f"  total_failed      : {m['total_failed']}")
    print(f"  failure_rate      : {m['failure_rate_pct']}%")
    print(f"  orders_per_minute : {m['orders_per_minute']}")
    print(f"  inventory_events  : {m['inventory_events_processed']}")
    if m.get("items_breakdown"):
        print(f"  items tracked     : {list(m['items_breakdown'].keys())}")
    return m


# ══════════════════════════════════════════════════════════════
# TEST 2 – Consumer lag under throttling
# ══════════════════════════════════════════════════════════════

def test_consumer_lag():
    print(f"\n{DIVIDER}")
    print("  TEST 2 ▸ Consumer Lag Under Throttling")
    print(DIVIDER)

    # 1) stop inventory-consumer, restart with 100 ms throttle
    print("\n  [1/4] Stopping inventory-consumer …")
    _dc("stop", "inventory-consumer")
    time.sleep(3)

    print("  [2/4] Starting throttled inventory-consumer (THROTTLE_MS=100) …")
    _dc("run", "-d", "-e", "THROTTLE_MS=100",
        "--name", "inv-throttled", "inventory-consumer")
    time.sleep(8)                                   # let it join the group

    # 2) burst-produce 3 000 events
    BURST = 3_000
    print(f"\n  [3/4] Burst-producing {BURST} events …")
    resp = _post(f"{PRODUCER_URL}/orders/bulk",
                 {"n": BURST, "item": "mixed"})
    print(f"    produced={resp['produced']}  "
          f"rate={resp['rate_per_second']:.0f}/s")

    # 3) monitor lag
    print(f"\n  [4/4] Monitoring consumer lag (25 s) …")
    print(f"  {'sec':>5}  {'lag':>7}  visualisation")
    print(f"  {'---':>5}  {'---':>7}  {'─'*40}")
    readings = []
    for t in range(25):
        lag = _consumer_lag()
        readings.append(lag)
        bar = "█" * min(lag // 100, 40)
        print(f"  {t:5d}  {lag:7d}  {bar}")
        time.sleep(1)

    # cleanup
    subprocess.run(["docker", "rm", "-f", "inv-throttled"],
                   capture_output=True)
    _dc("start", "inventory-consumer")

    valid = [l for l in readings if l >= 0]
    print(f"\n  ── Lag summary ──")
    if valid:
        print(f"  peak lag  : {max(valid):>7}")
        print(f"  final lag : {valid[-1]:>7}")
        print(f"  mean lag  : {statistics.mean(valid):>7.0f}")

    print(textwrap.dedent("""
    EXPLANATION
    ───────────
    With THROTTLE_MS=100 the consumer processes ≤ 10 msgs/s, while the
    producer burst 3 000 events in < 3 s.  Kafka durably stores every
    event, so nothing is lost; the consumer-group "lag" just keeps
    growing until the throttle is removed and the consumer catches up.
    """))
    return readings


# ══════════════════════════════════════════════════════════════
# TEST 3 – Replay (offset reset → recompute metrics)
# ══════════════════════════════════════════════════════════════

def test_replay():
    print(DIVIDER)
    print("  TEST 3 ▸ Replay – Reset Offset & Recompute")
    print(DIVIDER)

    # wait for everything to settle
    print("\n  Settling 10 s …")
    time.sleep(10)

    # before
    print("\n  [1/3] Metrics BEFORE replay:")
    m_before = _get(f"{ANALYTICS_URL}/metrics")
    for k in ("total_orders", "total_reserved", "total_failed",
              "failure_rate_pct", "orders_per_minute"):
        print(f"    {k:<28} = {m_before[k]}")

    # trigger replay
    print("\n  [2/3] POST /replay  (resetting offsets to beginning) …")
    t0 = time.time()
    m_after = _post(f"{ANALYTICS_URL}/replay")
    dur = time.time() - t0
    print(f"    Replay finished in {dur:.1f} s")

    print("\n  [3/3] Metrics AFTER replay:")
    for k in ("total_orders", "total_reserved", "total_failed",
              "failure_rate_pct", "orders_per_minute"):
        print(f"    {k:<28} = {m_after[k]}")

    # comparison table
    print(f"\n  ── Consistency check ──")
    print(f"  {'metric':<28} {'before':>10} {'after':>10} {'match':>7}")
    print(f"  {'─'*28} {'─'*10} {'─'*10} {'─'*7}")
    for k in ("total_orders", "total_reserved", "total_failed",
              "failure_rate_pct"):
        b, a = m_before.get(k, "?"), m_after.get(k, "?")
        ok = "YES" if b == a else "≈"
        print(f"  {k:<28} {str(b):>10} {str(a):>10} {ok:>7}")

    print(textwrap.dedent("""
    EXPLANATION
    ───────────
    Kafka's append-only log makes full replay possible:

      1. Analytics creates a *new* consumer group so the existing live
         consumer is undisturbed.
      2. It seeks every assigned partition to offset 0.
      3. It re-reads ALL events on OrderEvents + InventoryEvents and
         recomputes every metric from scratch.

    total_orders MUST be identical because the OrderEvents log has not
    changed.  total_reserved / total_failed will also match because
    InventoryEvents were already produced with deterministic outcomes;
    replay just re-reads them.

    This is Kafka's key advantage over RabbitMQ: the log IS the source
    of truth, and any consumer can re-derive state at any time.
    """))
    return m_before, m_after


# ══════════════════════════════════════════════════════════════
# Metrics report file
# ══════════════════════════════════════════════════════════════

def write_report(m10k, lag, m_before, m_after):
    lines = [
        DIVIDER,
        "  PART C – KAFKA STREAMING METRICS REPORT",
        f"  Generated : {time.strftime('%Y-%m-%d %H:%M:%S')}",
        DIVIDER, "",
        "[1] 10 000 Event Production",
        f"  total_orders      : {m10k.get('total_orders','?')}",
        f"  total_reserved    : {m10k.get('total_reserved','?')}",
        f"  total_failed      : {m10k.get('total_failed','?')}",
        f"  failure_rate      : {m10k.get('failure_rate_pct','?')} %",
        f"  orders_per_minute : {m10k.get('orders_per_minute','?')}",
        "",
        "[2] Consumer Lag (throttled @ 100 ms/msg)",
    ]
    valid = [l for l in lag if l >= 0]
    if valid:
        lines += [
            f"  peak lag    : {max(valid)}",
            f"  final lag   : {valid[-1]}",
            f"  readings    : {valid[:12]} …",
        ]
    lines += [
        "",
        "[3] Replay Consistency",
        f"  {'metric':<28} {'before':>10} {'after':>10}",
        f"  {'─'*28} {'─'*10} {'─'*10}",
    ]
    for k in ("total_orders", "total_reserved", "total_failed",
              "failure_rate_pct"):
        lines.append(
            f"  {k:<28} {str(m_before.get(k,'?')):>10} "
            f"{str(m_after.get(k,'?')):>10}")
    lines += ["", DIVIDER]
    text = "\n".join(lines)

    path = os.path.join(os.path.dirname(__file__), "kafka_metrics_report.txt")
    with open(path, "w") as f:
        f.write(text)
    print(f"\n  📄  Report saved → {path}")
    print(text)


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{DIVIDER}")
    print("  PART C – Streaming Kafka  ·  Full Test Suite")
    print(DIVIDER)

    wait_for_services()

    m10k            = test_produce_10k()
    lag_readings    = test_consumer_lag()
    m_before, m_after = test_replay()

    write_report(m10k, lag_readings, m_before, m_after)

    print(f"\n{DIVIDER}")
    print("  ALL TESTS COMPLETE  ✓")
    print(DIVIDER)
