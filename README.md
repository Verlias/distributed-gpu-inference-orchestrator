# Distributed GPU inference orchestrator

The project currently contains:

- `gateway`: the Go API gateway.
- `inference-server`: the official vLLM container configured for CPU development
  and NVIDIA GPU production.

The gateway also includes a concurrency-safe worker registry at
`gateway/internal/registry`. It tracks each worker's ID, vLLM address, model,
health and last-healthy time, lifecycle status, and active request count. It can
atomically select the least-loaded healthy worker for a requested model and
returns an idempotent release function for accurate request accounting.
Worker, registration, status, and registry-summary data models live alongside
the gateway's other API models in `gateway/internal/models`.


See [`inference-server/README.md`](inference-server/README.md) for setup and API
examples.
