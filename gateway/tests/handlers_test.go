package tests

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/handlers"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

type fakeInferenceClient struct {
	request  models.InferenceRequest
	response models.InferenceResponse
	err      error
}

func (f *fakeInferenceClient) Infer(
	_ context.Context, request models.InferenceRequest,
) (models.InferenceResponse, error) {
	f.request = request
	return f.response, f.err
}

func TestHealth(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	response := httptest.NewRecorder()

	handlers.Health(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected application/json content type, got %q", got)
	}
	if got := strings.TrimSpace(response.Body.String()); got != `{"status":"ok"}` {
		t.Errorf("unexpected response body: %s", got)
	}
}

func TestHealthRejectsUnsupportedMethods(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/health", nil)
	response := httptest.NewRecorder()

	handlers.Health(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, response.Code)
	}
	if got := response.Header().Get("Allow"); got != http.MethodGet {
		t.Errorf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}

func TestInference(t *testing.T) {
	temperature := 0.4
	client := &fakeInferenceClient{
		response: models.InferenceResponse{Message: "Hello from vLLM!"},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/inference",
		strings.NewReader(`{"prompt":"Say hello","max_tokens":64,"temperature":0.4}`),
	)
	response := httptest.NewRecorder()

	handlers.Inference(client)(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected application/json content type, got %q", got)
	}
	if got := strings.TrimSpace(response.Body.String()); got != `{"message":"Hello from vLLM!"}` {
		t.Errorf("unexpected response body: %s", got)
	}
	if client.request.Prompt != "Say hello" || client.request.MaxTokens != 64 {
		t.Errorf("unexpected client request: %#v", client.request)
	}
	if client.request.Temperature == nil || *client.request.Temperature != temperature {
		t.Errorf("unexpected temperature: %v", client.request.Temperature)
	}
}

func TestInferenceRejectsUnsupportedMethods(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/inference", nil)
	response := httptest.NewRecorder()

	handlers.Inference(&fakeInferenceClient{})(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, response.Code)
	}
	if got := response.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("expected Allow header %q, got %q", http.MethodPost, got)
	}
}

func TestInferenceValidatesRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid JSON", body: `{"prompt":`},
		{name: "missing prompt", body: `{}`},
		{name: "empty prompt", body: `{"prompt":"  "}`},
		{name: "excessive max tokens", body: `{"prompt":"hello","max_tokens":4097}`},
		{name: "invalid temperature", body: `{"prompt":"hello","temperature":2.1}`},
		{name: "unknown field", body: `{"prompt":"hello","unknown":true}`},
		{name: "multiple values", body: `{"prompt":"hello"} {}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/inference", strings.NewReader(test.body))
			response := httptest.NewRecorder()

			handlers.Inference(&fakeInferenceClient{})(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, response.Code)
			}
		})
	}
}

func TestInferenceHandlesClientFailure(t *testing.T) {
	client := &fakeInferenceClient{err: errors.New("upstream unavailable")}
	request := httptest.NewRequest(
		http.MethodPost, "/inference", strings.NewReader(`{"prompt":"hello"}`),
	)
	response := httptest.NewRecorder()

	handlers.Inference(client)(response, request)

	if response.Code != http.StatusBadGateway {
		t.Fatalf("expected status %d, got %d", http.StatusBadGateway, response.Code)
	}
}
