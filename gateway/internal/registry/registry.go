// Package registry keeps track of the vLLM workers available to the gateway.
package registry

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

var (
	ErrWorkerExists       = errors.New("worker already exists")
	ErrWorkerNotFound     = errors.New("worker not found")
	ErrWorkerUnavailable  = errors.New("worker is unavailable")
	ErrNoAvailableWorkers = errors.New("no available workers")
	ErrNoActiveRequests   = errors.New("worker has no active requests")
)

// Registry is an in-memory, concurrency-safe worker registry.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]models.Worker
	now     func() time.Time
}

// New creates an empty worker registry.
func New() *Registry {
	return &Registry{
		workers: make(map[string]models.Worker),
		now:     time.Now,
	}
}

// Register adds a worker. Use Upsert for idempotent worker registration.
func (r *Registry) Register(input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[normalized.ID]; exists {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerExists, normalized.ID)
	}

	worker := r.newWorker(normalized)
	r.workers[worker.ID] = worker
	return cloneWorker(worker), nil
}

// Upsert registers a new worker or refreshes an existing worker's metadata.
// Active request counts and the original registration time are preserved.
func (r *Registry) Upsert(input models.WorkerRegistration) (models.Worker, error) {
	normalized, err := normalizeRegistration(input)
	if err != nil {
		return models.Worker{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[normalized.ID]
	if !exists {
		worker = r.newWorker(normalized)
	} else {
		worker.Address = normalized.Address
		worker.Model = normalized.Model
		worker.Healthy = normalized.Healthy
		worker.Status = normalized.Status
		worker.UpdatedAt = r.now().UTC()
		if worker.Healthy {
			worker.LastHealthy = timePointer(worker.UpdatedAt)
		}
	}
	r.workers[worker.ID] = worker
	return cloneWorker(worker), nil
}

// Unregister removes a worker from the registry.
func (r *Registry) Unregister(id string) error {
	id = strings.TrimSpace(id)
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[id]; !exists {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	delete(r.workers, id)
	return nil
}

// Get returns a snapshot of one worker.
func (r *Registry) Get(id string) (models.Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, exists := r.workers[strings.TrimSpace(id)]
	if !exists {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	return cloneWorker(worker), nil
}

// List returns stable, ID-sorted snapshots of all workers.
func (r *Registry) List() []models.Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]models.Worker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, cloneWorker(worker))
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers
}

// SetHealth records the result of a worker health check. LastHealthy advances
// only after a successful check, so it remains useful while a worker is down.
func (r *Registry) SetHealth(id string, healthy bool) (models.Worker, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[strings.TrimSpace(id)]
	if !exists {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	now := r.now().UTC()
	worker.Healthy = healthy
	worker.UpdatedAt = now
	if healthy {
		worker.LastHealthy = timePointer(now)
	}
	r.workers[worker.ID] = worker
	return cloneWorker(worker), nil
}

// SetStatus updates a worker's lifecycle status.
func (r *Registry) SetStatus(id string, status models.WorkerStatus) (models.Worker, error) {
	if !validStatus(status) {
		return models.Worker{}, fmt.Errorf("invalid worker status %q", status)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[strings.TrimSpace(id)]
	if !exists {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	worker.Status = status
	worker.UpdatedAt = r.now().UTC()
	r.workers[worker.ID] = worker
	return cloneWorker(worker), nil
}

// StartRequest increments a specific worker's load and returns an idempotent
// release function. Only healthy, ready workers can start new requests.
func (r *Registry) StartRequest(id string) (models.Worker, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[strings.TrimSpace(id)]
	if !exists {
		return models.Worker{}, nil, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	if !workerAvailable(worker) {
		return models.Worker{}, nil, fmt.Errorf("%w: %s", ErrWorkerUnavailable, id)
	}

	worker.ActiveRequests++
	worker.UpdatedAt = r.now().UTC()
	r.workers[worker.ID] = worker
	return cloneWorker(worker), r.releaseOnce(worker.ID), nil
}

// Acquire selects the least-loaded healthy, ready worker for a model and
// atomically reserves one request slot. An empty model matches any model.
func (r *Registry) Acquire(model string) (models.Worker, func(), error) {
	model = strings.TrimSpace(model)
	r.mu.Lock()
	defer r.mu.Unlock()

	var selected *models.Worker
	for _, candidate := range r.workers {
		if !workerAvailable(candidate) || (model != "" && candidate.Model != model) {
			continue
		}
		if selected == nil || candidate.ActiveRequests < selected.ActiveRequests ||
			(candidate.ActiveRequests == selected.ActiveRequests && candidate.ID < selected.ID) {
			copy := candidate
			selected = &copy
		}
	}
	if selected == nil {
		return models.Worker{}, nil, fmt.Errorf("%w for model %q", ErrNoAvailableWorkers, model)
	}

	selected.ActiveRequests++
	selected.UpdatedAt = r.now().UTC()
	r.workers[selected.ID] = *selected
	return cloneWorker(*selected), r.releaseOnce(selected.ID), nil
}

// FinishRequest decrements a worker's active request count.
func (r *Registry) FinishRequest(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finishRequestLocked(strings.TrimSpace(id))
}

// Summary returns aggregate counts for monitoring and health endpoints.
func (r *Registry) Summary() models.WorkerRegistrySummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := models.WorkerRegistrySummary{TotalWorkers: len(r.workers)}
	for _, worker := range r.workers {
		if worker.Healthy {
			result.HealthyWorkers++
		} else {
			result.UnhealthyWorkers++
		}
		if workerAvailable(worker) {
			result.AvailableWorkers++
		}
		result.ActiveRequests += worker.ActiveRequests
	}
	return result
}

func (r *Registry) newWorker(input models.WorkerRegistration) models.Worker {
	now := r.now().UTC()
	worker := models.Worker{
		ID:           input.ID,
		Address:      input.Address,
		Model:        input.Model,
		Healthy:      input.Healthy,
		Status:       input.Status,
		RegisteredAt: now,
		UpdatedAt:    now,
	}
	if worker.Healthy {
		worker.LastHealthy = timePointer(now)
	}
	return worker
}

func (r *Registry) releaseOnce(id string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = r.FinishRequest(id)
		})
	}
}

func (r *Registry) finishRequestLocked(id string) error {
	worker, exists := r.workers[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	if worker.ActiveRequests == 0 {
		return fmt.Errorf("%w: %s", ErrNoActiveRequests, id)
	}
	worker.ActiveRequests--
	worker.UpdatedAt = r.now().UTC()
	r.workers[worker.ID] = worker
	return nil
}

func normalizeRegistration(input models.WorkerRegistration) (models.WorkerRegistration, error) {
	input.ID = strings.TrimSpace(input.ID)
	input.Address = strings.TrimRight(strings.TrimSpace(input.Address), "/")
	input.Model = strings.TrimSpace(input.Model)
	if input.Status == "" {
		input.Status = models.WorkerStatusReady
	}

	if input.ID == "" {
		return models.WorkerRegistration{}, errors.New("worker ID is required")
	}
	parsedAddress, err := url.Parse(input.Address)
	if err != nil || (parsedAddress.Scheme != "http" && parsedAddress.Scheme != "https") || parsedAddress.Host == "" {
		return models.WorkerRegistration{}, fmt.Errorf("worker address must be a valid HTTP(S) URL: %q", input.Address)
	}
	if input.Model == "" {
		return models.WorkerRegistration{}, errors.New("worker model is required")
	}
	if !validStatus(input.Status) {
		return models.WorkerRegistration{}, fmt.Errorf("invalid worker status %q", input.Status)
	}
	return input, nil
}

func validStatus(status models.WorkerStatus) bool {
	switch status {
	case models.WorkerStatusStarting, models.WorkerStatusReady,
		models.WorkerStatusDraining, models.WorkerStatusOffline:
		return true
	default:
		return false
	}
}

func workerAvailable(worker models.Worker) bool {
	return worker.Healthy && worker.Status == models.WorkerStatusReady
}

func cloneWorker(worker models.Worker) models.Worker {
	if worker.LastHealthy != nil {
		worker.LastHealthy = timePointer(*worker.LastHealthy)
	}
	return worker
}

func timePointer(value time.Time) *time.Time {
	return &value
}
