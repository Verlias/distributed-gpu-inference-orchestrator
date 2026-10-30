package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/api"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/service"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/store"
)

func main() {
	redisDatabase, err := strconv.Atoi(environmentOrDefault("REDIS_DB", "0"))
	if err != nil || redisDatabase < 0 {
		log.Fatalf("REDIS_DB must be a non-negative integer")
	}
	redisStore := store.NewRedis(store.RedisConfig{
		Address:   environmentOrDefault("REDIS_ADDR", "localhost:6379"),
		Password:  os.Getenv("REDIS_PASSWORD"),
		Database:  redisDatabase,
		KeyPrefix: environmentOrDefault("REDIS_KEY_PREFIX", "gpu-orchestrator"),
	})
	defer redisStore.Close()
	leaseTTL, err := time.ParseDuration(environmentOrDefault("WORKER_LEASE_TTL", "10s"))
	if err != nil || leaseTTL < time.Millisecond {
		log.Fatalf("WORKER_LEASE_TTL must be at least 1ms")
	}

	startupContext, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStartup()
	if err := redisStore.Ping(startupContext); err != nil {
		log.Fatalf("connect to Redis: %v", err)
	}

	registryService := service.New(redisStore, service.WithLeaseTTL(leaseTTL))
	server := &http.Server{
		Addr:              environmentOrDefault("REGISTRY_LISTEN_ADDR", ":8081"),
		Handler:           api.NewHandler(registryService).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownSignal, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go reapExpiredWorkers(shutdownSignal, registryService, minDuration(leaseTTL/2, 5*time.Second))
	go func() {
		<-shutdownSignal.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("registry shutdown failed: %v", err)
		}
	}()

	log.Printf("registry API listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("registry server stopped: %v", err)
	}
}

func reapExpiredWorkers(ctx context.Context, registryService *service.Registry, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reapContext, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := registryService.ReapExpired(reapContext); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("reap expired worker leases: %v", err)
			}
			cancel()
		}
	}
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
