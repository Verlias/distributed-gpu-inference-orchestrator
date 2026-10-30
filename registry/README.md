# Worker Registry Service

The registry is a standalone Go HTTP API backed by Redis. Controllers and
workers populate it through the API; no worker state is held in process memory.

## Run locally

```bash
docker compose up --build
```

The API listens on `http://localhost:8081` and Redis listens on port `6379`.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Verify the API can reach Redis |
| `POST` | `/workers` | Register a new worker |
| `PUT` | `/workers/{id}` | Idempotently register or update a worker endpoint |
| `POST` | `/workers/{id}/heartbeat` | Update health and load telemetry |
| `GET` | `/workers` | List workers |
| `GET` | `/workers/{id}` | Get one worker |
| `DELETE` | `/workers/{id}` | Remove a worker |
| `GET` | `/workers/select` | Select the least-loaded healthy worker |
| `GET` | `/summary` | Return aggregate health and load information |

Register a worker:

```bash
curl -X POST http://localhost:8081/workers \
  -H 'Content-Type: application/json' \
  -d '{"worker_id":"worker-1","endpoint":"http://worker-1:8000"}'
```

Record a heartbeat:

```bash
curl -X POST http://localhost:8081/workers/worker-1/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"health_status":"healthy","waiting_requests":2,"kv_cache_utilization":0.45}'
```

## Configuration

| Variable | Default |
| --- | --- |
| `REGISTRY_LISTEN_ADDR` | `:8081` |
| `REDIS_ADDR` | `localhost:6379` |
| `REDIS_PASSWORD` | empty |
| `REDIS_DB` | `0` |
| `REDIS_KEY_PREFIX` | `gpu-orchestrator` |

Run unit tests with `go test -race ./...`. To include the real Redis integration
test, start Redis and run:

```bash
REDIS_INTEGRATION_ADDR=localhost:6379 go test ./store -run Integration
```
