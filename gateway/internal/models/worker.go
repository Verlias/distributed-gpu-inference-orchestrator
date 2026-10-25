package models

import "time"

// WorkerStatus describes whether an inference worker can receive new traffic.
type WorkerStatus string

const (
	WorkerStatusStarting WorkerStatus = "starting"
	WorkerStatusReady    WorkerStatus = "ready"
	WorkerStatusDraining WorkerStatus = "draining"
	WorkerStatusOffline  WorkerStatus = "offline"
)

// Worker is a point-in-time view of a registered inference worker.
type Worker struct {
	ID             string       `json:"id"`
	Address        string       `json:"address"`
	Model          string       `json:"model"`
	Healthy        bool         `json:"healthy"`
	LastHealthy    *time.Time   `json:"last_healthy,omitempty"`
	Status         WorkerStatus `json:"status"`
	ActiveRequests int          `json:"active_requests"`
	RegisteredAt   time.Time    `json:"registered_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

// WorkerRegistration contains the worker-owned fields used during registration.
// Runtime-owned values such as active requests and timestamps are maintained by
// the registry and therefore are not accepted from a worker.
type WorkerRegistration struct {
	ID      string       `json:"id"`
	Address string       `json:"address"`
	Model   string       `json:"model"`
	Healthy bool         `json:"healthy"`
	Status  WorkerStatus `json:"status"`
}

// WorkerRegistrySummary contains aggregate worker registry state.
type WorkerRegistrySummary struct {
	TotalWorkers     int `json:"total_workers"`
	HealthyWorkers   int `json:"healthy_workers"`
	UnhealthyWorkers int `json:"unhealthy_workers"`
	ActiveRequests   int `json:"active_requests"`
	AvailableWorkers int `json:"available_workers"`
}
