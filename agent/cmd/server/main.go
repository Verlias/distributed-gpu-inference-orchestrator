package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	monitor "github.com/Verlias/distributed-gpu-inference-orchestrator/agent"
)

func main() {
	pollInterval := durationEnvironment("HEALTH_POLL_INTERVAL", 2*time.Second)
	requestTimeout := durationEnvironment("HEALTH_REQUEST_TIMEOUT", time.Second)
	agent, err := monitor.New(monitor.Config{
		WorkerID: os.Getenv("WORKER_ID"), VLLMEndpoint: os.Getenv("VLLM_ENDPOINT"),
		RegistryURL:  environmentOrDefault("REGISTRY_BASE_URL", "http://localhost:8081"),
		PollInterval: pollInterval,
	}, &http.Client{Timeout: requestTimeout})
	if err != nil {
		log.Fatalf("configure health agent: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("monitoring worker %s every %s", os.Getenv("WORKER_ID"), pollInterval)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("health agent stopped: %v", err)
	}
}

func durationEnvironment(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Fatalf("%s must be a positive Go duration", name)
	}
	return parsed
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
