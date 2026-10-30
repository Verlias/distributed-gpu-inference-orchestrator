package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/store"
)

func TestRedisStoreIntegration(t *testing.T) {
	address := os.Getenv("REDIS_INTEGRATION_ADDR")
	if address == "" {
		t.Skip("set REDIS_INTEGRATION_ADDR to run against a real Redis server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("registry-integration-%d", time.Now().UnixNano())
	redisStore := store.NewRedis(store.RedisConfig{Address: address, KeyPrefix: prefix})
	if err := redisStore.Ping(ctx); err != nil {
		t.Fatalf("ping Redis: %v", err)
	}

	worker := models.Worker{ID: "worker-1", Endpoint: "http://worker:8000", HealthStatus: models.HealthStatusUnknown}
	worker.LeaseExpiresAt = time.Now().UTC().Add(10 * time.Second)
	created, err := redisStore.Create(ctx, worker, 10*time.Second)
	if err != nil || created.ID != worker.ID {
		t.Fatalf("create worker: worker=%#v err=%v", created, err)
	}
	if _, err := redisStore.Create(ctx, worker, 10*time.Second); !errors.Is(err, store.ErrWorkerExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}

	heartbeatAt := time.Now().UTC().Truncate(time.Microsecond)
	updated, err := redisStore.UpdateHeartbeat(ctx, worker.ID, models.Heartbeat{
		HealthStatus: models.HealthStatusHealthy, WaitingRequests: 2, KVCacheUtilization: 0.55,
	}, heartbeatAt, heartbeatAt.Add(10*time.Second), 10*time.Second)
	if err != nil || updated.LastHeartbeat == nil || !updated.LastHeartbeat.Equal(heartbeatAt) || updated.WaitingRequests != 2 {
		t.Fatalf("update heartbeat: worker=%#v err=%v", updated, err)
	}

	workers, err := redisStore.List(ctx, time.Now().UTC())
	if err != nil || len(workers) != 1 {
		t.Fatalf("list workers: workers=%#v err=%v", workers, err)
	}
	expiring := models.Worker{
		ID: "expiring-worker", Endpoint: "http://expiring:8000", HealthStatus: models.HealthStatusUnknown,
		LeaseExpiresAt: time.Now().UTC().Add(100 * time.Millisecond),
	}
	if _, err := redisStore.Create(ctx, expiring, 100*time.Millisecond); err != nil {
		t.Fatalf("create expiring worker: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := redisStore.Get(ctx, expiring.ID); !errors.Is(err, store.ErrWorkerNotFound) {
		t.Fatalf("expected Redis TTL to remove worker, got %v", err)
	}
	if err := redisStore.ReapExpired(ctx, time.Now().UTC()); err != nil {
		t.Fatalf("reap expired leases: %v", err)
	}
	if err := redisStore.Delete(ctx, worker.ID); err != nil {
		t.Fatalf("delete worker: %v", err)
	}
	if _, err := redisStore.Get(ctx, worker.ID); !errors.Is(err, store.ErrWorkerNotFound) {
		t.Fatalf("expected missing worker, got %v", err)
	}
}
