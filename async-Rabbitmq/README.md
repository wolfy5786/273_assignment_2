# Part B: Async Messaging with RabbitMQ

## Architecture

```
OrderService (Flask :8080)
  POST /order → saves to local dict → publishes "OrderPlaced" to RabbitMQ
       │
       ▼
  [orders exchange (topic)] ──order.placed──► [order_placed_queue] (DLX configured)
       │
       ▼
InventoryService (Flask :8082 + consumer thread)
  - Checks idempotency (processed_order_ids set)
  - Reserves stock or fails
  - Publishes "InventoryReserved" or "InventoryFailed"
       │
       ▼
  [inventory exchange (topic)] ──inventory.reserved──► [inventory_reserved_queue]
       │
       ▼
NotificationService (Flask :8081 + consumer thread)
  - Logs confirmation notification

Malformed messages → basic_nack(requeue=False) → [dlx_exchange] → [dead_letter_queue]
```

## Quick Start

```bash
cd async-Rabbitmq
docker compose up --build -d

# Wait ~15 seconds for RabbitMQ to become healthy
docker compose ps
```

## Smoke Test

```bash
# Place an order
curl -s -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{"item":"widget-A","quantity":1}' | python3 -m json.tool

# Wait for async processing
sleep 2

# Check inventory (widget-A should be 99)
curl -s http://localhost:8082/inventory | python3 -m json.tool

# Check notification was received
curl -s http://localhost:8081/notifications | python3 -m json.tool
```

## API Endpoints

| Service | Endpoint | Description |
|---------|----------|-------------|
| OrderService | `POST /order` | Place an order (`{"item":"widget-A","quantity":1}`) |
| OrderService | `GET /orders` | List all placed orders |
| OrderService | `GET /health` | Health check |
| InventoryService | `GET /inventory` | Current stock levels |
| InventoryService | `GET /processed` | List of processed order IDs |
| InventoryService | `GET /health` | Health check |
| NotificationService | `GET /notifications` | List of sent notifications |
| NotificationService | `GET /health` | Health check |

## Running Tests

```bash
pip install -r tests/requirements.txt

# Test 1: Idempotency — same order_id twice, stock decremented only once
python3 tests/test_idempotency.py

# Test 2: DLQ — invalid JSON and missing fields routed to dead_letter_queue
python3 tests/test_dlq.py

# Test 3: Backlog drain — stop inventory, publish 10 orders, restart, verify drain
python3 tests/test_backlog_drain.py
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Exchange type | `topic` | Flexible routing keys (`order.placed`, `inventory.reserved`) |
| Message acknowledgment | Manual (`auto_ack=False`) | Messages survive consumer crashes; required for backlog drain |
| Idempotency | In-memory `set` of processed `order_id`s | Simple, sufficient for demo; production would use Redis/DB |
| DLQ | `x-dead-letter-exchange` on queue + `basic_nack(requeue=False)` | Standard RabbitMQ pattern for poison messages |
| Consumer threading | `threading.Thread(daemon=True)` for pika, main thread for Flask | Allows both HTTP endpoints and message consumption |
| Connection per publish | New `BlockingConnection` each publish | Thread-safe with Flask's threaded model |
| `prefetch_count` | 1 | Sequential processing, no race conditions on idempotency set |

## Idempotency Strategy

InventoryService maintains an in-memory `processed_order_ids` set. Before reserving stock, it checks whether the incoming `order_id` already exists in this set:

- **If duplicate:** ACK the message, skip processing — no inventory change
- **If new:** Reserve stock, add `order_id` to the set, publish result, ACK

This guarantees exactly-once processing even when RabbitMQ redelivers the same message (e.g., consumer crash before ACK). `prefetch_count=1` ensures sequential processing, preventing race conditions on the set.

## DLQ / Poison Message Handling

Messages that fail validation are sent to the dead letter queue via:

1. Queues are declared with `x-dead-letter-exchange: dlx_exchange`
2. Consumer calls `basic_nack(requeue=False)` for:
   - Invalid JSON (unparseable body)
   - Missing required fields (`order_id`, `item`, `quantity`)
3. RabbitMQ routes nacked messages to `dlx_exchange` → `dead_letter_queue`

The consumer stays healthy and continues processing valid messages.

## Cleanup

```bash
docker compose down -v
```
