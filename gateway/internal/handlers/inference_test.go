package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInference(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/inference", nil)
	response := httptest.NewRecorder()

	Inference(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected application/json content type, got %q", got)
	}
	if got := strings.TrimSpace(response.Body.String()); got != `{"message":"hello"}` {
		t.Errorf("unexpected response body: %s", got)
	}
}

func TestInferenceRejectsUnsupportedMethods(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/inference", nil)
	response := httptest.NewRecorder()

	Inference(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, response.Code)
	}
	if got := response.Header().Get("Allow"); got != http.MethodPost {
		t.Errorf("expected Allow header %q, got %q", http.MethodPost, got)
	}
}
