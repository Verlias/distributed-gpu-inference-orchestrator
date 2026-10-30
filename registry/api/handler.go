// Package api exposes the worker registry over HTTP.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/service"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/store"
)

const maxRequestBodyBytes = 1 << 20

type registryService interface {
	Ping(context.Context) error
	Register(context.Context, models.WorkerRegistration) (models.Worker, error)
	Upsert(context.Context, models.WorkerRegistration) (models.Worker, error)
	RecordHeartbeat(context.Context, string, models.Heartbeat) (models.Worker, error)
	Get(context.Context, string) (models.Worker, error)
	List(context.Context) ([]models.Worker, error)
	Delete(context.Context, string) error
	Select(context.Context) (models.Worker, error)
	Summary(context.Context) (models.WorkerRegistrySummary, error)
}

type Handler struct {
	registry registryService
}

func NewHandler(registry registryService) *Handler {
	return &Handler{registry: registry}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /workers", h.register)
	mux.HandleFunc("GET /workers", h.list)
	mux.HandleFunc("GET /workers/select", h.selectWorker)
	mux.HandleFunc("PUT /workers/{id}", h.upsert)
	mux.HandleFunc("GET /workers/{id}", h.get)
	mux.HandleFunc("DELETE /workers/{id}", h.delete)
	mux.HandleFunc("POST /workers/{id}/heartbeat", h.heartbeat)
	mux.HandleFunc("GET /summary", h.summary)
	return mux
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if err := h.registry.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "Redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var input models.WorkerRegistration
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	worker, err := h.registry.Register(r.Context(), input)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, worker)
}

func (h *Handler) upsert(w http.ResponseWriter, r *http.Request) {
	var input models.WorkerRegistration
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	id := strings.TrimSpace(r.PathValue("id"))
	if input.ID != "" && input.ID != id {
		writeError(w, http.StatusBadRequest, "worker_id must match the URL path")
		return
	}
	input.ID = id
	worker, err := h.registry.Upsert(r.Context(), input)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (h *Handler) heartbeat(w http.ResponseWriter, r *http.Request) {
	var input models.Heartbeat
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	worker, err := h.registry.RecordHeartbeat(r.Context(), r.PathValue("id"), input)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	worker, err := h.registry.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	workers, err := h.registry.List(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	if workers == nil {
		workers = []models.Worker{}
	}
	writeJSON(w, http.StatusOK, workers)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	if err := h.registry.Delete(r.Context(), r.PathValue("id")); err != nil {
		handleServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) selectWorker(w http.ResponseWriter, r *http.Request) {
	worker, err := h.registry.Select(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, worker)
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.registry.Summary(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}

func handleServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrWorkerExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrWorkerNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, service.ErrNoAvailableWorkers):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		log.Printf("registry request failed: %v", err)
		writeError(w, http.StatusInternalServerError, "internal registry error")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("encode registry response: %v", err)
	}
}
