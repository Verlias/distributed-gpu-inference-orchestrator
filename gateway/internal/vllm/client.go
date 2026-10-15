package vllm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

// Client invokes vLLM's OpenAI-compatible API.
type Client struct {
	baseURL    *url.URL
	model      string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a vLLM client. A nil HTTP client uses a two-minute timeout.
func NewClient(baseURL, model, apiKey string, httpClient *http.Client) (*Client, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("invalid vLLM base URL %q", baseURL)
	}
	if strings.TrimSpace(model) == "" {
		return nil, fmt.Errorf("vLLM model name cannot be empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Minute}
	}

	return &Client{
		baseURL:    parsedURL,
		model:      model,
		apiKey:     apiKey,
		httpClient: httpClient,
	}, nil
}

// Infer sends a prompt to vLLM and returns the first generated message.
func (c *Client) Infer(ctx context.Context, input models.InferenceRequest) (models.InferenceResponse, error) {
	payload, err := json.Marshal(models.VLLMChatCompletionRequest{
		Model:       c.model,
		Messages:    []models.VLLMChatMessage{{Role: "user", Content: input.Prompt}},
		MaxTokens:   input.EffectiveMaxTokens(),
		Temperature: input.Temperature,
	})
	if err != nil {
		return models.InferenceResponse{}, fmt.Errorf("encode vLLM request: %w", err)
	}

	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/v1/chat/completions"
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload),
	)
	if err != nil {
		return models.InferenceResponse{}, fmt.Errorf("create vLLM request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return models.InferenceResponse{}, fmt.Errorf("call vLLM: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return models.InferenceResponse{}, fmt.Errorf(
			"vLLM returned %s: %s", response.Status, strings.TrimSpace(string(body)),
		)
	}

	var completion models.VLLMChatCompletionResponse
	if err := json.NewDecoder(response.Body).Decode(&completion); err != nil {
		return models.InferenceResponse{}, fmt.Errorf("decode vLLM response: %w", err)
	}
	if len(completion.Choices) == 0 {
		return models.InferenceResponse{}, fmt.Errorf("vLLM response contained no choices")
	}

	return models.InferenceResponse{Message: completion.Choices[0].Message.Content}, nil
}
