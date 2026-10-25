package registry

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

func TestRegisterAndHealthTracking(t *testing.T) {
	now := time.Date(2026, time.October, 23, 12, 0, 0, 0, time.UTC)
	registry := New()
	registry.now = func() time.Time { return now }

	worker, err := registry.Register(models.WorkerRegistration{
		ID: "worker-1", Address: "http://worker-1:8000/", Model: "llama", Healthy: true,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if worker.Address != "http://worker-1:8000" || worker.Status != models.WorkerStatusReady {
		t.Fatalf("unexpected normalized worker: %#v", worker)
	}
	if worker.LastHealthy == nil || !worker.LastHealthy.Equal(now) {
		t.Fatalf("expected last healthy %v, got %v", now, worker.LastHealthy)
	}

	now = now.Add(time.Minute)
	worker, err = registry.SetHealth("worker-1", false)
	if err != nil {
		t.Fatalf("mark worker unhealthy: %v", err)
	}
	if worker.Healthy || !worker.LastHealthy.Equal(now.Add(-time.Minute)) {
		t.Fatalf("unhealthy update lost last healthy time: %#v", worker)
	}

	now = now.Add(time.Minute)
	worker, err = registry.SetHealth("worker-1", true)
	if err != nil {
		t.Fatalf("mark worker healthy: %v", err)
	}
	if !worker.Healthy || !worker.LastHealthy.Equal(now) {
		t.Fatalf("healthy update did not advance last healthy: %#v", worker)
	}
}

func TestRegisterValidatesWorkerAndRejectsDuplicates(t *testing.T) {
	registry := New()
	valid := models.WorkerRegistration{ID: "worker-1", Address: "http://worker:8000", Model: "llama"}
	if _, err := registry.Register(valid); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if _, err := registry.Register(valid); !errors.Is(err, ErrWorkerExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}

	tests := []models.WorkerRegistration{
		{Address: "http://worker:8000", Model: "llama"},
		{ID: "worker", Address: "worker:8000", Model: "llama"},
		{ID: "worker", Address: "http://worker:8000"},
		{ID: "worker", Address: "http://worker:8000", Model: "llama", Status: "unknown"},
	}
	for _, input := range tests {
		if _, err := registry.Register(input); err == nil {
			t.Errorf("expected validation error for %#v", input)
		}
	}
}

func TestAcquireChoosesHealthyReadyLeastLoadedWorker(t *testing.T) {
	registry := New()
	registrations := []models.WorkerRegistration{
		{ID: "worker-b", Address: "http://b:8000", Model: "llama", Healthy: true},
		{ID: "worker-a", Address: "http://a:8000", Model: "llama", Healthy: true},
		{ID: "unhealthy", Address: "http://unhealthy:8000", Model: "llama"},
		{ID: "other-model", Address: "http://other:8000", Model: "mistral", Healthy: true},
	}
	for _, registration := range registrations {
		if _, err := registry.Register(registration); err != nil {
			t.Fatalf("register worker: %v", err)
		}
	}

	first, releaseFirst, err := registry.Acquire("llama")
	if err != nil {
		t.Fatalf("acquire first worker: %v", err)
	}
	if first.ID != "worker-a" {
		t.Fatalf("expected stable tie-break to choose worker-a, got %s", first.ID)
	}
	second, releaseSecond, err := registry.Acquire("llama")
	if err != nil {
		t.Fatalf("acquire second worker: %v", err)
	}
	if second.ID != "worker-b" {
		t.Fatalf("expected least-loaded worker-b, got %s", second.ID)
	}

	releaseFirst()
	releaseFirst() // Release is deliberately idempotent.
	releaseSecond()
	if summary := registry.Summary(); summary.ActiveRequests != 0 {
		t.Fatalf("expected no active requests after release, got %#v", summary)
	}
}

func TestStatusAndSummary(t *testing.T) {
	registry := New()
	_, _ = registry.Register(models.WorkerRegistration{
		ID: "ready", Address: "http://ready:8000", Model: "llama", Healthy: true,
	})
	_, _ = registry.Register(models.WorkerRegistration{
		ID: "down", Address: "http://down:8000", Model: "llama", Healthy: false,
	})
	if _, err := registry.SetStatus("ready", models.WorkerStatusDraining); err != nil {
		t.Fatalf("drain worker: %v", err)
	}

	summary := registry.Summary()
	if summary.TotalWorkers != 2 || summary.HealthyWorkers != 1 ||
		summary.UnhealthyWorkers != 1 || summary.AvailableWorkers != 0 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if _, _, err := registry.Acquire("llama"); !errors.Is(err, ErrNoAvailableWorkers) {
		t.Fatalf("expected no available worker, got %v", err)
	}
}

func TestConcurrentRequestAccounting(t *testing.T) {
	registry := New()
	_, err := registry.Register(models.WorkerRegistration{
		ID: "worker", Address: "http://worker:8000", Model: "llama", Healthy: true,
	})
	if err != nil {
		t.Fatalf("register worker: %v", err)
	}

	const requests = 100
	releases := make(chan func(), requests)
	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, release, acquireErr := registry.Acquire("llama")
			if acquireErr != nil {
				t.Errorf("acquire worker: %v", acquireErr)
				return
			}
			releases <- release
		}()
	}
	wait.Wait()
	close(releases)

	if got := registry.Summary().ActiveRequests; got != requests {
		t.Fatalf("expected %d active requests, got %d", requests, got)
	}
	for release := range releases {
		release()
	}
	if got := registry.Summary().ActiveRequests; got != 0 {
		t.Fatalf("expected all requests released, got %d", got)
	}
}

func TestUpsertPreservesRequestCount(t *testing.T) {
	registry := New()
	input := models.WorkerRegistration{
		ID: "worker", Address: "http://worker:8000", Model: "llama", Healthy: true,
	}
	if _, err := registry.Register(input); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	_, release, err := registry.StartRequest("worker")
	if err != nil {
		t.Fatalf("start request: %v", err)
	}

	input.Address = "http://worker:9000"
	worker, err := registry.Upsert(input)
	if err != nil {
		t.Fatalf("upsert worker: %v", err)
	}
	if worker.Address != input.Address || worker.ActiveRequests != 1 {
		t.Fatalf("upsert did not preserve load: %#v", worker)
	}
	release()
}
