package handlers

import (
	"context"
	"log"
	"net/http"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/internal/models"
)

type inferenceClient interface {
	Infer(context.Context, models.InferenceRequest) (models.InferenceResponse, error)
}

// Inference accepts gateway requests and forwards them to vLLM.
func Inference(client inferenceClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}

		var input models.InferenceRequest
		if err := decodeJSON(w, r, &input); err != nil {
			http.Error(w, "invalid JSON request body", http.StatusBadRequest)
			return
		}
		if err := input.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		result, err := client.Infer(r.Context(), input)
		if err != nil {
			log.Printf("vLLM inference failed: %v", err)
			http.Error(w, "inference service unavailable", http.StatusBadGateway)
			return
		}

		if err := writeJSON(w, http.StatusOK, result); err != nil {
			log.Printf("encode inference response: %v", err)
		}
	}
}
