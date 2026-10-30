# vLLM worker health agent

One agent monitors one vLLM endpoint. It registers the endpoint once, checks
vLLM health and load, and sends a compact heartbeat to the registry every two
seconds. The registry owns the heartbeat timestamp and Redis lease.

Configuration:

| Variable | Required/default |
| --- | --- |
| `WORKER_ID` | required |
| `VLLM_ENDPOINT` | required |
| `REGISTRY_BASE_URL` | `http://localhost:8081` |
| `HEALTH_POLL_INTERVAL` | `2s` |
| `HEALTH_REQUEST_TIMEOUT` | `1s` |

The image is sidecar-ready: in Kubernetes, place it in the same pod as vLLM,
give the pod a stable worker ID, and register an endpoint reachable by the
gateway.
