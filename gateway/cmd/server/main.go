package main

import (
	"log"
	"net/http"
	"os"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/handlers"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/vllm"
)

func main() {
	vllmClient, err := vllm.NewClient(
		environmentOrDefault("VLLM_BASE_URL", "http://localhost:8000"),
		environmentOrDefault("VLLM_MODEL", "orchestrator-model"),
		os.Getenv("VLLM_API_KEY"),
		nil,
	)
	if err != nil {
		log.Fatalf("configure vLLM client: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handlers.Health)
	mux.HandleFunc("/inference", handlers.Inference(vllmClient))

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
