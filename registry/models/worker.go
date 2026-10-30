// Package models contains the worker registry's data contracts.
package models

import "time"

// HealthStatus is the latest known health state of a worker.
type HealthStatus string

const (
	HealthStatusUnknown   HealthStatus = "unknown"
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// Worker is the registry's latest view of a vLLM worker. KVCacheUtilization is
// represented as a normalized value from 0.0 (empty) to 1.0 (fully utilized).
type Worker struct {
	ID                 string       `json:"worker_id"`
	Endpoint           string       `json:"endpoint"`
	HealthStatus       HealthStatus `json:"health_status"`
	LastHeartbeat      *time.Time   `json:"last_heartbeat,omitempty"`
	LeaseExpiresAt     time.Time    `json:"lease_expires_at"`
	WaitingRequests    int          `json:"waiting_requests"`
	KVCacheUtilization float64      `json:"kv_cache_utilization"`
}

// WorkerRegistration contains the identity fields used to discover a worker.
// Runtime health and load values are supplied separately through Heartbeat.
type WorkerRegistration struct {
	ID       string `json:"worker_id"`
	Endpoint string `json:"endpoint"`
}

// Heartbeat contains the latest health and load telemetry reported for a worker.
type Heartbeat struct {
	HealthStatus       HealthStatus `json:"health_status"`
	WaitingRequests    int          `json:"waiting_requests"`
	KVCacheUtilization float64      `json:"kv_cache_utilization"`
}

// WorkerRegistrySummary contains aggregate worker registry state.
type WorkerRegistrySummary struct {
	TotalWorkers         int     `json:"total_workers"`
	HealthyWorkers       int     `json:"healthy_workers"`
	UnhealthyWorkers     int     `json:"unhealthy_workers"`
	UnknownWorkers       int     `json:"unknown_workers"`
	TotalWaitingRequests int     `json:"total_waiting_requests"`
	AverageKVUtilization float64 `json:"average_kv_cache_utilization"`
}
