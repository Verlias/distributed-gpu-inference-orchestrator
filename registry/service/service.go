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
	store    store.WorkerStore
	now      func() time.Time
	leaseTTL time.Duration
}

const DefaultLeaseTTL = 10 * time.Second

type Option func(*Registry)

func WithLeaseTTL(ttl time.Duration) Option {
	return func(registry *Registry) { registry.leaseTTL = ttl }
}

func WithClock(now func() time.Time) Option {
	return func(registry *Registry) { registry.now = now }
}

func New(workerStore store.WorkerStore, options ...Option) *Registry {
	registry := &Registry{store: workerStore, now: time.Now, leaseTTL: DefaultLeaseTTL}
	for _, option := range options {
		option(registry)
	}
	return registry
}

func (r *Registry) Ping(ctx context.Context) error {
	return r.store.Ping(ctx)
}

func (r *Registry) Register(ctx context.Context, input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}
	now := r.now().UTC()
	return r.store.Create(ctx, models.Worker{
		ID:             normalized.ID,
		Endpoint:       normalized.Endpoint,
		HealthStatus:   models.HealthStatusUnknown,
		LeaseExpiresAt: now.Add(r.leaseTTL),
	}, r.leaseTTL)
}

func (r *Registry) Upsert(ctx context.Context, input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}
	now := r.now().UTC()
	return r.store.UpsertIdentity(ctx, normalized, now.Add(r.leaseTTL), r.leaseTTL)
}

func (r *Registry) RecordHeartbeat(ctx context.Context, id string, heartbeat models.Heartbeat) (models.Worker, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return models.Worker{}, fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	if err := validateHeartbeat(heartbeat); err != nil {
		return models.Worker{}, err
	}
	now := r.now().UTC()
	return r.store.UpdateHeartbeat(ctx, id, heartbeat, now, now.Add(r.leaseTTL), r.leaseTTL)
}

func (r *Registry) Get(ctx context.Context, id string) (models.Worker, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return models.Worker{}, fmt.Errorf("%w: worker ID is required", ErrInvalidInput)
	}
	worker, err := r.store.Get(ctx, id)
	if err != nil {
		return models.Worker{}, err
	}
	if !worker.LeaseExpiresAt.After(r.now().UTC()) {
		_ = r.store.Delete(ctx, id)
		return models.Worker{}, fmt.Errorf("%w: %s", store.ErrWorkerNotFound, id)
	}
	return worker, nil
}

func (r *Registry) List(ctx context.Context) ([]models.Worker, error) {
	workers, err := r.store.List(ctx, r.now().UTC())
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
	workers, err := r.List(ctx)
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
	workers, err := r.List(ctx)
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

func (r *Registry) ReapExpired(ctx context.Context) error {
	return r.store.ReapExpired(ctx, r.now().UTC())
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
