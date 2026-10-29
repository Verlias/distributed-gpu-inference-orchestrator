// Package service implements worker registry business rules.
package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/store"
)

var (
	ErrInvalidInput       = errors.New("invalid input")
	ErrNoAvailableWorkers = errors.New("no available workers")
)

// Registry coordinates validation, selection, summaries, and persistence.
type Registry struct {
	store store.WorkerStore
	now   func() time.Time
}

func New(workerStore store.WorkerStore) *Registry {
	return &Registry{store: workerStore, now: time.Now}
}

func (r *Registry) Ping(ctx context.Context) error {
	return r.store.Ping(ctx)
}

func (r *Registry) Register(ctx context.Context, input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}
	return r.store.Create(ctx, models.Worker{
		ID:           normalized.ID,
		Endpoint:     normalized.Endpoint,
		HealthStatus: models.HealthStatusUnknown,
	})
}

func (r *Registry) Upsert(ctx context.Context, input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}
	return r.store.UpsertIdentity(ctx, normalized)
}

func (r *Registry) RecordHeartbeat(ctx context.Context, id string, heartbeat models.Heartbeat) (models.Worker, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return models.Worker{}, fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	if err := validateHeartbeat(heartbeat); err != nil {
		return models.Worker{}, err
	}
	return r.store.UpdateHeartbeat(ctx, id, heartbeat, r.now().UTC())
}

func (r *Registry) Get(ctx context.Context, id string) (models.Worker, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return models.Worker{}, fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	return r.store.Get(ctx, id)
}

func (r *Registry) List(ctx context.Context) ([]models.Worker, error) {
	workers, err := r.store.List(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers, nil
}

func (r *Registry) Delete(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	return r.store.Delete(ctx, id)
}

func (r *Registry) Select(ctx context.Context) (models.Worker, error) {
	workers, err := r.store.List(ctx)
	if err != nil {
		return models.Worker{}, err
	}
	var selected *models.Worker
	for _, candidate := range workers {
		if candidate.HealthStatus != models.HealthStatusHealthy {
			continue
		}
		if selected == nil || lessLoaded(candidate, *selected) {
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return models.Worker{}, ErrNoAvailableWorkers
	}
	return *selected, nil
}

func (r *Registry) Summary(ctx context.Context) (models.WorkerRegistrySummary, error) {
	workers, err := r.store.List(ctx)
	if err != nil {
		return models.WorkerRegistrySummary{}, err
	}
	result := models.WorkerRegistrySummary{TotalWorkers: len(workers)}
	for _, worker := range workers {
		switch worker.HealthStatus {
		case models.HealthStatusHealthy:
			result.HealthyWorkers++
		case models.HealthStatusUnhealthy:
			result.UnhealthyWorkers++
		default:
			result.UnknownWorkers++
		}
		result.TotalWaitingRequests += worker.WaitingRequests
		result.AverageKVUtilization += worker.KVCacheUtilization
	}
	if result.TotalWorkers > 0 {
		result.AverageKVUtilization /= float64(result.TotalWorkers)
	}
	return result, nil
}

func normalizeRegistration(input models.WorkerRegistration) (models.WorkerRegistration, error) {
	input.ID = strings.TrimSpace(input.ID)
	input.Endpoint = strings.TrimRight(strings.TrimSpace(input.Endpoint), "/")
	if input.ID == "" {
		return models.WorkerRegistration{}, fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	parsedEndpoint, err := url.Parse(input.Endpoint)
	if err != nil || (parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") || parsedEndpoint.Host == "" {
		return models.WorkerRegistration{}, fmt.Errorf("%w: endpoint must be a valid HTTP(S) URL", ErrInvalidInput)
	}
	return input, nil
}

func validateHeartbeat(heartbeat models.Heartbeat) error {
	if heartbeat.HealthStatus != models.HealthStatusHealthy && heartbeat.HealthStatus != models.HealthStatusUnhealthy {
		return fmt.Errorf("%w: health status must be %q or %q", ErrInvalidInput, models.HealthStatusHealthy, models.HealthStatusUnhealthy)
	}
	if heartbeat.WaitingRequests < 0 {
		return fmt.Errorf("%w: waiting requests cannot be negative", ErrInvalidInput)
	}
	if heartbeat.KVCacheUtilization < 0 || heartbeat.KVCacheUtilization > 1 {
		return fmt.Errorf("%w: KV-cache utilization must be between 0 and 1", ErrInvalidInput)
	}
	return nil
}

func lessLoaded(left, right models.Worker) bool {
	if left.WaitingRequests != right.WaitingRequests {
		return left.WaitingRequests < right.WaitingRequests
	}
	if left.KVCacheUtilization != right.KVCacheUtilization {
		return left.KVCacheUtilization < right.KVCacheUtilization
	}
	return left.ID < right.ID
}
