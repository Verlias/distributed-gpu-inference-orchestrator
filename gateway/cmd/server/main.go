package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/handlers"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/orchestrator"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/registry"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/vllm"
)

func main() {
	vllmClient, err := vllm.NewDynamicClient(
		environmentOrDefault("VLLM_MODEL", "orchestrator-model"),
		os.Getenv("VLLM_API_KEY"),
		nil,
	)
	if err != nil {
		log.Fatalf("configure vLLM client: %v", err)
	}
	registryClient, err := registry.NewClient(
		environmentOrDefault("REGISTRY_BASE_URL", "http://localhost:8081"),
		&http.Client{Timeout: time.Second},
	)
	if err != nil {
		log.Fatalf("configure registry client: %v", err)
	}
	inferenceClient := orchestrator.NewClient(registryClient, vllmClient)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handlers.Health)
	mux.HandleFunc("/inference", handlers.Inference(inferenceClient))

	log.Println("API gateway listening on :8090")
	if err := http.ListenAndServe(":8090", mux); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

func environmentOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
