# sync-REST

A synchronous REST microservices demo with three services: **Order Service**, **Inventory Service**, and **Notification Service**. The order service coordinates with the other two to process orders.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Project layout

- `order_service/` – Order API (port 8080), calls notification and inventory
- `inventory_service/` – Inventory reserve API (port 8082)
- `notification_service/` – Notification API (port 8081)
- `tests/` – Integration tests for the order service

Docker Compose uses the **parent directory** as the build context (for `logging_utils` and `setup.py`), so all commands below should be run from the **`sync-REST`** directory.

---

## Build

Build all images (no containers started):

```bash
docker-compose build
```

Build and start services in one step:

```bash
docker-compose up --build
```

---

## Run services

Start the three microservices in the background:

```bash
docker-compose up -d
```

Service URLs:

| Service              | URL                     |
|----------------------|-------------------------|
| Order Service        | http://localhost:8080   |
| Notification Service | http://localhost:8081   |
| Inventory Service    | http://localhost:8082   |

Health checks:

- Order: `GET http://localhost:8080/health`
- Notification: `GET http://localhost:8081/health`
- Inventory: `GET http://localhost:8082/health`

Stop the services (containers removed):

```bash
docker-compose down
```

---

## Run tests

Start all services and run the order-service integration tests, then stop everything:

```bash
docker-compose --profile test up --build --abort-on-container-exit
```

- `--profile test` includes the `order-service-tests` container.
- `--abort-on-container-exit` stops all containers when the test container exits.
- Test results appear in the `order-service-tests` container logs.

Run tests with services already up (start tests only):

```bash
docker-compose --profile test run --rm order-service-tests
```

---

## Delete / cleanup

Remove containers, networks, and optionally images and volumes.

**Containers and networks only (keeps images):**

```bash
docker-compose down
```

**Containers, networks, and images built by this project:**

```bash
docker-compose down --rmi local
```

**Containers, networks, and volumes:**

```bash
docker-compose down -v
```

**Full cleanup (containers, networks, images, volumes):**

```bash
docker-compose down -v --rmi local
```

---

## Quick reference

| Goal              | Command |
|-------------------|--------|
| Build             | `docker-compose build` |
| Run services      | `docker-compose up -d` |
| Run services + tests | `docker-compose --profile test up --build --abort-on-container-exit` |
| Stop & remove     | `docker-compose down` |
| Clean including images | `docker-compose down --rmi local` |
