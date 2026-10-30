package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/api"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/service"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/store"
)

func TestRegistryLifecycle(t *testing.T) {
	workerStore := newFakeStore()
	registry := service.New(workerStore)
	ctx := context.Background()

	worker, err := registry.Register(ctx, models.WorkerRegistration{ID: "worker-b", Endpoint: "http://worker-b:8000/"})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if worker.Endpoint != "http://worker-b:8000" || worker.HealthStatus != models.HealthStatusUnknown {
		t.Fatalf("unexpected registered worker: %#v", worker)
	}
	if _, err := registry.Register(ctx, models.WorkerRegistration{ID: "worker-b", Endpoint: "http://worker-b:8000"}); !errors.Is(err, store.ErrWorkerExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}

	_, _ = registry.Register(ctx, models.WorkerRegistration{ID: "worker-a", Endpoint: "http://worker-a:8000"})
	_, _ = registry.RecordHeartbeat(ctx, "worker-a", models.Heartbeat{
		HealthStatus: models.HealthStatusHealthy, WaitingRequests: 1, KVCacheUtilization: 0.3,
	})
	_, _ = registry.RecordHeartbeat(ctx, "worker-b", models.Heartbeat{
		HealthStatus: models.HealthStatusHealthy, WaitingRequests: 2, KVCacheUtilization: 0.1,
	})

	selected, err := registry.Select(ctx)
	if err != nil || selected.ID != "worker-a" {
		t.Fatalf("unexpected selected worker: worker=%#v err=%v", selected, err)
	}
	summary, err := registry.Summary(ctx)
	if err != nil || summary.TotalWorkers != 2 || summary.HealthyWorkers != 2 || summary.TotalWaitingRequests != 3 {
		t.Fatalf("unexpected summary: summary=%#v err=%v", summary, err)
	}
	if err := registry.Delete(ctx, "worker-a"); err != nil {
		t.Fatalf("delete worker: %v", err)
	}
	if _, err := registry.Get(ctx, "worker-a"); !errors.Is(err, store.ErrWorkerNotFound) {
		t.Fatalf("expected deleted worker to be missing, got %v", err)
	}
}

func TestRegistryValidatesRequests(t *testing.T) {
	registry := service.New(newFakeStore())
	ctx := context.Background()
	if _, err := registry.Register(ctx, models.WorkerRegistration{Endpoint: "http://worker:8000"}); !errors.Is(err, service.ErrInvalidInput) {
		t.Fatalf("expected missing ID validation error, got %v", err)
	}
	if _, err := registry.Register(ctx, models.WorkerRegistration{ID: "worker", Endpoint: "worker:8000"}); !errors.Is(err, service.ErrInvalidInput) {
		t.Fatalf("expected endpoint validation error, got %v", err)
	}
	_, _ = registry.Register(ctx, models.WorkerRegistration{ID: "worker", Endpoint: "http://worker:8000"})
	if _, err := registry.RecordHeartbeat(ctx, "worker", models.Heartbeat{
		HealthStatus: models.HealthStatusHealthy, KVCacheUtilization: 1.1,
	}); !errors.Is(err, service.ErrInvalidInput) {
		t.Fatalf("expected heartbeat validation error, got %v", err)
	}
}

func TestWorkerLeaseRenewsAndExpires(t *testing.T) {
	now := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	workerStore := newFakeStore()
	registry := service.New(workerStore, service.WithLeaseTTL(10*time.Second), service.WithClock(func() time.Time {
		return now
	}))
	ctx := context.Background()

	worker, err := registry.Register(ctx, models.WorkerRegistration{ID: "worker-1", Endpoint: "http://worker:8000"})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if !worker.LeaseExpiresAt.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("unexpected initial lease: %v", worker.LeaseExpiresAt)
	}

	now = now.Add(2 * time.Second)
	worker, err = registry.RecordHeartbeat(ctx, worker.ID, models.Heartbeat{HealthStatus: models.HealthStatusHealthy})
	if err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}
	if !worker.LeaseExpiresAt.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("heartbeat did not renew lease: %v", worker.LeaseExpiresAt)
	}

	now = now.Add(11 * time.Second)
	if _, err := registry.Get(ctx, worker.ID); !errors.Is(err, store.ErrWorkerNotFound) {
		t.Fatalf("expected expired worker to be missing, got %v", err)
	}
	if _, err := registry.Select(ctx); !errors.Is(err, service.ErrNoAvailableWorkers) {
		t.Fatalf("expected no available workers, got %v", err)
	}
}

func TestUnknownAndUnhealthyWorkersRemainVisibleButAreNotSelected(t *testing.T) {
	registry := service.New(newFakeStore())
	ctx := context.Background()
	if _, err := registry.Register(ctx, models.WorkerRegistration{
		ID: "worker-1", Endpoint: "http://worker:8000",
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := registry.Select(ctx); !errors.Is(err, service.ErrNoAvailableWorkers) {
		t.Fatalf("expected unknown worker to be excluded, got %v", err)
	}
	if _, err := registry.RecordHeartbeat(ctx, "worker-1", models.Heartbeat{
		HealthStatus: models.HealthStatusUnhealthy,
	}); err != nil {
		t.Fatalf("record unhealthy heartbeat: %v", err)
	}
	workers, err := registry.List(ctx)
	if err != nil || len(workers) != 1 || workers[0].HealthStatus != models.HealthStatusUnhealthy {
		t.Fatalf("expected visible unhealthy worker, workers=%#v err=%v", workers, err)
	}
	if _, err := registry.Select(ctx); !errors.Is(err, service.ErrNoAvailableWorkers) {
		t.Fatalf("expected unhealthy worker to be excluded, got %v", err)
	}
}

func TestRegistryAPIWorkerLifecycle(t *testing.T) {
	handler := api.NewHandler(service.New(newFakeStore())).Routes()

	response := requestJSON(t, handler, http.MethodPost, "/workers", `{
        "worker_id":"worker-1","endpoint":"http://worker-1:8000/"
    }`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("expected registration status 201, got %d", response.StatusCode)
	}
	response.Body.Close()

	response = requestJSON(t, handler, http.MethodPost, "/workers/worker-1/heartbeat", `{
        "health_status":"healthy","waiting_requests":3,"kv_cache_utilization":0.4
    }`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected heartbeat status 200, got %d", response.StatusCode)
	}
	var heartbeatWorker models.Worker
	decodeResponse(t, response, &heartbeatWorker)
	if heartbeatWorker.LastHeartbeat == nil || heartbeatWorker.WaitingRequests != 3 {
		t.Fatalf("unexpected heartbeat response: %#v", heartbeatWorker)
	}

	response = requestJSON(t, handler, http.MethodGet, "/workers/select", "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected selection status 200, got %d", response.StatusCode)
	}
	var selected models.Worker
	decodeResponse(t, response, &selected)
	if selected.ID != "worker-1" {
		t.Fatalf("unexpected selected worker: %#v", selected)
	}

	response = requestJSON(t, handler, http.MethodGet, "/summary", "")
	var summary models.WorkerRegistrySummary
	decodeResponse(t, response, &summary)
	if summary.TotalWorkers != 1 || summary.TotalWaitingRequests != 3 {
		t.Fatalf("unexpected API summary: %#v", summary)
	}

	response = requestJSON(t, handler, http.MethodDelete, "/workers/worker-1", "")
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("expected deletion status 204, got %d", response.StatusCode)
	}
	response.Body.Close()

	response = requestJSON(t, handler, http.MethodGet, "/workers/worker-1", "")
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("expected missing worker status 404, got %d", response.StatusCode)
	}
	response.Body.Close()
}

func TestRegistryAPIMapsConflictsAndValidation(t *testing.T) {
	handler := api.NewHandler(service.New(newFakeStore())).Routes()
	body := `{"worker_id":"worker","endpoint":"http://worker:8000"}`
	first := requestJSON(t, handler, http.MethodPost, "/workers", body)
	first.Body.Close()
	duplicate := requestJSON(t, handler, http.MethodPost, "/workers", body)
	if duplicate.StatusCode != http.StatusConflict {
		t.Fatalf("expected duplicate status 409, got %d", duplicate.StatusCode)
	}
	duplicate.Body.Close()
	invalid := requestJSON(t, handler, http.MethodPost, "/workers/worker/heartbeat", `{"health_status":"healthy","waiting_requests":-1}`)
	if invalid.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected validation status 400, got %d", invalid.StatusCode)
	}
	invalid.Body.Close()
}

func requestJSON(t *testing.T, handler http.Handler, method, endpoint, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Result()
}

func decodeResponse(t *testing.T, response *http.Response, destination any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

type fakeStore struct {
	mu      sync.RWMutex
	workers map[string]models.Worker
}

func newFakeStore() *fakeStore {
	return &fakeStore{workers: make(map[string]models.Worker)}
}

func (s *fakeStore) Ping(context.Context) error { return nil }
func (s *fakeStore) Close() error               { return nil }

func (s *fakeStore) Create(_ context.Context, worker models.Worker, _ time.Duration) (models.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workers[worker.ID]; exists {
		return models.Worker{}, store.ErrWorkerExists
	}
	s.workers[worker.ID] = worker
	return worker, nil
}

func (s *fakeStore) UpsertIdentity(
	_ context.Context, input models.WorkerRegistration, leaseExpiresAt time.Time, _ time.Duration,
) (models.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, exists := s.workers[input.ID]
	if !exists {
		worker = models.Worker{ID: input.ID, HealthStatus: models.HealthStatusUnknown}
	}
	worker.Endpoint = input.Endpoint
	worker.LeaseExpiresAt = leaseExpiresAt
	s.workers[input.ID] = worker
	return worker, nil
}

func (s *fakeStore) UpdateHeartbeat(
	_ context.Context, id string, heartbeat models.Heartbeat, at, leaseExpiresAt time.Time, _ time.Duration,
) (models.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, exists := s.workers[id]
	if !exists {
		return models.Worker{}, store.ErrWorkerNotFound
	}
	worker.HealthStatus = heartbeat.HealthStatus
	worker.LastHeartbeat = &at
	worker.LeaseExpiresAt = leaseExpiresAt
	worker.WaitingRequests = heartbeat.WaitingRequests
	worker.KVCacheUtilization = heartbeat.KVCacheUtilization
	s.workers[id] = worker
	return worker, nil
}

func (s *fakeStore) Get(_ context.Context, id string) (models.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	worker, exists := s.workers[id]
	if !exists {
		return models.Worker{}, store.ErrWorkerNotFound
	}
	return worker, nil
}

func (s *fakeStore) List(_ context.Context, now time.Time) ([]models.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	workers := make([]models.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		if worker.LeaseExpiresAt.After(now) {
			workers = append(workers, worker)
		}
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })
	return workers, nil
}

func (s *fakeStore) ReapExpired(_ context.Context, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, worker := range s.workers {
		if !worker.LeaseExpiresAt.After(now) {
			delete(s.workers, id)
		}
	}
	return nil
}

func (s *fakeStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workers[id]; !exists {
		return store.ErrWorkerNotFound
	}
	delete(s.workers, id)
	return nil
}
