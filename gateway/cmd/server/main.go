package main

import (
	"log"
	"net/http"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/handlers"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handlers.Health)
	mux.HandleFunc("/inference", handlers.Inference)

	log.Println("API gateway listening on :8090")
	if err := http.ListenAndServe(":8090", mux); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
