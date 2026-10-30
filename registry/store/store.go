// Package store defines persistence used by the registry service.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
)

var (
	ErrWorkerExists   = errors.New("worker already exists")
	ErrWorkerNotFound = errors.New("worker not found")
)

// WorkerStore persists worker identity and heartbeat telemetry.
type WorkerStore interface {
	Ping(context.Context) error
	Create(context.Context, models.Worker, time.Duration) (models.Worker, error)
	UpsertIdentity(context.Context, models.WorkerRegistration, time.Time, time.Duration) (models.Worker, error)
	UpdateHeartbeat(context.Context, string, models.Heartbeat, time.Time, time.Time, time.Duration) (models.Worker, error)
	Get(context.Context, string) (models.Worker, error)
	List(context.Context, time.Time) ([]models.Worker, error)
	Delete(context.Context, string) error
	ReapExpired(context.Context, time.Time) error
	Close() error
}
