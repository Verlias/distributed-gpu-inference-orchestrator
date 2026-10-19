# vLLM inference server

This folder runs the official vLLM OpenAI-compatible server image directly. It
does not contain or build a custom Python server. Development defaults to the
CPU image and `Qwen/Qwen3-0.6B`, served to clients as `orchestrator-model`.

## Run on CPU

```bash
cp .env.example .env
docker compose up
```

The first start downloads about 1.5 GB of model weights into the persistent
`huggingface-cache` Docker volume. Once `/health` responds successfully, make a
request with:

```bash
curl http://localhost:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "orchestrator-model",
    "messages": [{"role": "user", "content": "Say hello in one sentence."}],
    "max_tokens": 64
  }'
```

Useful endpoints:

- `GET /health` for container and orchestration health checks.
- `GET /v1/models` to inspect the loaded model.
- `POST /v1/chat/completions` for chat inference.
- `POST /v1/completions` for text completions.

## Run on NVIDIA GPUs in production

The GPU override swaps to the official CUDA image, requests GPU access, enables
host IPC, and retains the same API configuration:

```bash
VLLM_DTYPE=auto \
docker compose -f compose.yaml -f compose.gpu.yaml up
```

For a larger production model, change `MODEL_ID` and size
`VLLM_TENSOR_PARALLEL_SIZE` to the number of GPUs assigned to the replica. The
gateway can continue using the stable served name `orchestrator-model`.

## Connect the gateway

The Go gateway defaults to a vLLM server on `http://localhost:8000`. Override
these variables when the services run on different hosts or networks:

```bash
VLLM_BASE_URL=http://inference-server:8000
VLLM_MODEL=orchestrator-model
```

With both services running, invoke inference through the gateway:

```bash
curl http://localhost:8090/inference \
  -H 'Content-Type: application/json' \
  -d '{
    "prompt": "Say hello in one sentence.",
    "max_tokens": 64,
    "temperature": 0.4
  }'
```
