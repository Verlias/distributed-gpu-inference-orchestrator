package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/vllm"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func testHTTPClient(transport roundTripFunc) *http.Client {
	return &http.Client{Transport: transport}
}

func TestClientInfer(t *testing.T) {
	httpClient := testHTTPClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.String() != "http://vllm:8000/v1/chat/completions" {
			t.Errorf("unexpected request URL %q", r.URL.String())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("unexpected authorization header %q", got)
		}

		var request models.VLLMChatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if request.Model != "orchestrator-model" {
			t.Errorf("unexpected model %q", request.Model)
		}
		if len(request.Messages) != 1 || request.Messages[0].Content != "Say hello" {
			t.Errorf("unexpected messages: %#v", request.Messages)
		}
		if request.MaxTokens != models.DefaultMaxTokens {
			t.Errorf("expected default max tokens %d, got %d", models.DefaultMaxTokens, request.MaxTokens)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"choices":[{"message":{"role":"assistant","content":"Hello!"}}]}`,
			)),
		}, nil
	})

	client, err := vllm.NewClient("http://vllm:8000", "orchestrator-model", "test-key", httpClient)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	response, err := client.Infer(context.Background(), models.InferenceRequest{Prompt: "Say hello"})
	if err != nil {
		t.Fatalf("infer: %v", err)
	}
	if response.Message != "Hello!" {
		t.Errorf("unexpected response %q", response.Message)
	}
}

func TestClientInferReturnsUpstreamError(t *testing.T) {
	httpClient := testHTTPClient(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("model unavailable")),
		}, nil
	})
	client, err := vllm.NewClient("http://vllm:8000", "orchestrator-model", "", httpClient)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	_, err = client.Infer(context.Background(), models.InferenceRequest{Prompt: "hello"})
	if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") {
		t.Fatalf("expected upstream status error, got %v", err)
	}
}

func TestClientInferRejectsResponseWithoutChoices(t *testing.T) {
	httpClient := testHTTPClient(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"choices":[]}`)),
		}, nil
	})
	client, err := vllm.NewClient("http://vllm:8000", "orchestrator-model", "", httpClient)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	_, err = client.Infer(context.Background(), models.InferenceRequest{Prompt: "hello"})
	if err == nil || !strings.Contains(err.Error(), "no choices") {
		t.Fatalf("expected no choices error, got %v", err)
	}
}

func TestNewClientValidatesConfiguration(t *testing.T) {
	if _, err := vllm.NewClient("not-a-url", "model", "", nil); err == nil {
		t.Error("expected invalid URL error")
	}
	if _, err := vllm.NewClient("http://localhost:8000", "", "", nil); err == nil {
		t.Error("expected empty model error")
	}
}
