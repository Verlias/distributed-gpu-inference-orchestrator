package models

import (
	"errors"
	"fmt"
	"strings"
)

const (
	DefaultMaxTokens = 128
	MaxTokens        = 4096
)

// InferenceRequest is the gateway's public inference request.
type InferenceRequest struct {
	Prompt      string   `json:"prompt"`
	MaxTokens   int      `json:"max_tokens,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
}

func (r InferenceRequest) Validate() error {
	if strings.TrimSpace(r.Prompt) == "" {
		return errors.New("prompt is required")
	}
	if r.MaxTokens < 0 || r.MaxTokens > MaxTokens {
		return fmt.Errorf("max_tokens must be between 1 and %d when provided", MaxTokens)
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return errors.New("temperature must be between 0 and 2")
	}
	return nil
}

func (r InferenceRequest) EffectiveMaxTokens() int {
	if r.MaxTokens == 0 {
		return DefaultMaxTokens
	}
	return r.MaxTokens
}

// InferenceResponse is the gateway's public inference response.
type InferenceResponse struct {
	Message string `json:"message"`
}
