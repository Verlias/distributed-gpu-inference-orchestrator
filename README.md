# Distributed GPU inference orchestrator

The project currently contains:

- `gateway`: the Go API gateway.
- `inference-server`: the official vLLM container configured for CPU development
  and NVIDIA GPU production.

The project also includes a Redis-backed worker registry API at `registry`.
Controllers and workers populate it over HTTP with worker identity, vLLM
endpoint, health, heartbeat, queue-depth, and KV-cache telemetry. See
[`registry/README.md`](registry/README.md) for endpoints and local setup.

See [`inference-server/README.md`](inference-server/README.md) for setup and API
examples.


## System Architecture

![GPU Inference Orchestrator Architecture](docs/architecture.svg)
