package tests

import (
	"testing"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

func TestInferenceRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		request models.InferenceRequest
		valid   bool
	}{
		{name: "valid", request: models.InferenceRequest{Prompt: "hello"}, valid: true},
		{name: "missing prompt", request: models.InferenceRequest{}, valid: false},
		{name: "excessive tokens", request: models.InferenceRequest{Prompt: "hello", MaxTokens: models.MaxTokens + 1}, valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.request.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("Validate() error = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestInferenceRequestEffectiveMaxTokens(t *testing.T) {
	if got := (models.InferenceRequest{}).EffectiveMaxTokens(); got != models.DefaultMaxTokens {
		t.Fatalf("expected default %d, got %d", models.DefaultMaxTokens, got)
	}
	if got := (models.InferenceRequest{MaxTokens: 64}).EffectiveMaxTokens(); got != 64 {
		t.Fatalf("expected configured value 64, got %d", got)
	}
}
