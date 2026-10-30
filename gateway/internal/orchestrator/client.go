// Package orchestrator connects registry selection to vLLM inference.
package orchestrator

import (
	"context"
	"fmt"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/registry"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/vllm"
)

type Client struct {
	registry *registry.Client
	vllm     *vllm.Client
}

func NewClient(registryClient *registry.Client, vllmClient *vllm.Client) *Client {
	return &Client{registry: registryClient, vllm: vllmClient}
}

func (c *Client) Infer(
	ctx context.Context, input models.InferenceRequest,
) (models.InferenceResponse, error) {
	worker, err := c.registry.Select(ctx)
	if err != nil {
		return models.InferenceResponse{}, err
	}
	response, err := c.vllm.InferAt(ctx, worker.Endpoint, input)
	if err != nil {
		return models.InferenceResponse{}, fmt.Errorf("worker %s inference: %w", worker.ID, err)
	}
	return response, nil
}
