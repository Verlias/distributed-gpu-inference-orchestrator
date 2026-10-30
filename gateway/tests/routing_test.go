package tests

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/orchestrator"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/registry"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/vllm"
)

func TestOrchestratorRoutesToSelectedWorker(t *testing.T) {
	registryCalls := 0
	vllmCalls := 0
	httpClient := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "registry.test":
			registryCalls++
			if request.URL.Path != "/workers/select" {
				t.Fatalf("unexpected registry path %q", request.URL.Path)
			}
			return testResponse(http.StatusOK, `{"worker_id":"worker-2","endpoint":"http://worker-2.test:8000"}`), nil
		case "worker-2.test:8000":
			vllmCalls++
			if request.URL.Path != "/v1/chat/completions" {
				t.Fatalf("unexpected vLLM path %q", request.URL.Path)
			}
			return testResponse(http.StatusOK, `{"choices":[{"message":{"role":"assistant","content":"routed"}}]}`), nil
		default:
			t.Fatalf("unexpected host %q", request.URL.Host)
			return nil, nil
		}
	})
	registryClient, err := registry.NewClient("http://registry.test", httpClient)
	if err != nil {
		t.Fatalf("create registry client: %v", err)
	}
	vllmClient, err := vllm.NewDynamicClient("orchestrator-model", "", httpClient)
	if err != nil {
		t.Fatalf("create vLLM client: %v", err)
	}
	client := orchestrator.NewClient(registryClient, vllmClient)

	result, err := client.Infer(context.Background(), models.InferenceRequest{Prompt: "hello"})
	if err != nil {
		t.Fatalf("infer: %v", err)
	}
	if result.Message != "routed" || registryCalls != 1 || vllmCalls != 1 {
		t.Fatalf("unexpected result=%#v registryCalls=%d vllmCalls=%d", result, registryCalls, vllmCalls)
	}
}

func testResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)),
	}
}
