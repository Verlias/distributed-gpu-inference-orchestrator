// Package agent monitors one vLLM server and reports its health and load to the registry.
package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var errWorkerMissing = errors.New("worker is not registered")

type Config struct {
	WorkerID     string
	VLLMEndpoint string
	RegistryURL  string
	PollInterval time.Duration
}

type Agent struct {
	workerID     string
	vllmURL      *url.URL
	registryURL  *url.URL
	pollInterval time.Duration
	httpClient   *http.Client
}

type workerRegistration struct {
	ID       string `json:"worker_id"`
	Endpoint string `json:"endpoint"`
}

type heartbeat struct {
	HealthStatus       string  `json:"health_status"`
	WaitingRequests    int     `json:"waiting_requests"`
	KVCacheUtilization float64 `json:"kv_cache_utilization"`
}

func New(config Config, httpClient *http.Client) (*Agent, error) {
	config.WorkerID = strings.TrimSpace(config.WorkerID)
	if config.WorkerID == "" {
		return nil, errors.New("worker ID is required")
	}
	vllmURL, err := parseHTTPURL(config.VLLMEndpoint)
	if err != nil {
		return nil, fmt.Errorf("vLLM endpoint: %w", err)
	}
	registryURL, err := parseHTTPURL(config.RegistryURL)
	if err != nil {
		return nil, fmt.Errorf("registry URL: %w", err)
	}
	if config.PollInterval <= 0 {
		return nil, errors.New("poll interval must be positive")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: time.Second}
	}
	return &Agent{
		workerID: config.WorkerID, vllmURL: vllmURL, registryURL: registryURL,
		pollInterval: config.PollInterval, httpClient: httpClient,
	}, nil
}

// Run registers the worker, polls immediately, and continues until the context is canceled.
func (a *Agent) Run(ctx context.Context) error {
	registered := false
	for {
		if !registered {
			if err := a.Register(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("register worker %s: %v", a.workerID, err)
			} else {
				registered = true
			}
		}
		if registered {
			if err := a.Poll(ctx); err != nil {
				if errors.Is(err, errWorkerMissing) {
					registered = false
					if registerErr := a.Register(ctx); registerErr == nil {
						registered = true
						if retryErr := a.Poll(ctx); retryErr != nil && ctx.Err() == nil {
							log.Printf("retry worker %s heartbeat: %v", a.workerID, retryErr)
						}
					} else if ctx.Err() == nil {
						log.Printf("re-register worker %s: %v", a.workerID, registerErr)
					}
				} else if ctx.Err() == nil {
					log.Printf("report worker %s health: %v", a.workerID, err)
				}
			}
		}

		timer := time.NewTimer(a.jitteredPollInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (a *Agent) jitteredPollInterval() time.Duration {
	factor := 0.9 + rand.Float64()*0.2
	return time.Duration(float64(a.pollInterval) * factor)
}

func (a *Agent) Register(ctx context.Context) error {
	payload := workerRegistration{ID: a.workerID, Endpoint: strings.TrimRight(a.vllmURL.String(), "/")}
	return a.sendJSON(ctx, http.MethodPut, a.registryPath("workers", url.PathEscape(a.workerID)), payload, nil)
}

// Poll checks vLLM and sends one compact heartbeat. A failed vLLM check is reported as unhealthy.
func (a *Agent) Poll(ctx context.Context) error {
	report := heartbeat{HealthStatus: "unhealthy"}
	if err := a.checkHealth(ctx); err == nil {
		waiting, utilization, metricsErr := a.readMetrics(ctx)
		if metricsErr == nil {
			report.HealthStatus = "healthy"
			report.WaitingRequests = waiting
			report.KVCacheUtilization = utilization
		}
	}
	err := a.sendJSON(ctx, http.MethodPost,
		a.registryPath("workers", url.PathEscape(a.workerID), "heartbeat"), report, nil)
	return err
}

func (a *Agent) checkHealth(ctx context.Context) error {
	response, err := a.get(ctx, a.vllmPath("health"))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("vLLM health returned %s", response.Status)
	}
	return nil
}

func (a *Agent) readMetrics(ctx context.Context) (int, float64, error) {
	response, err := a.get(ctx, a.vllmPath("metrics"))
	if err != nil {
		return 0, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return 0, 0, fmt.Errorf("vLLM metrics returned %s", response.Status)
	}
	return parseMetrics(io.LimitReader(response.Body, 4<<20))
}

func parseMetrics(reader io.Reader) (int, float64, error) {
	waiting := 0.0
	utilization := 0.0
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.SplitN(fields[0], "{", 2)[0]
		if name != "vllm:num_requests_waiting" && name != "vllm:kv_cache_usage_perc" {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, 0, fmt.Errorf("invalid %s metric value %q", name, fields[1])
		}
		if name == "vllm:num_requests_waiting" {
			waiting += value
		} else if value > utilization {
			utilization = value
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, fmt.Errorf("read vLLM metrics: %w", err)
	}
	if waiting < 0 || waiting > float64(math.MaxInt) || utilization < 0 || utilization > 1 {
		return 0, 0, errors.New("vLLM metrics are outside their valid range")
	}
	return int(waiting), utilization, nil
}

func (a *Agent) sendJSON(ctx context.Context, method, endpoint string, payload, destination any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := a.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return errWorkerMissing
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("registry returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	if destination != nil {
		return json.NewDecoder(response.Body).Decode(destination)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return nil
}

func (a *Agent) get(ctx context.Context, endpoint string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	return a.httpClient.Do(request)
}

func (a *Agent) registryPath(parts ...string) string { return joinURL(a.registryURL, parts...) }
func (a *Agent) vllmPath(parts ...string) string     { return joinURL(a.vllmURL, parts...) }

func joinURL(base *url.URL, parts ...string) string {
	result := *base
	pathParts := []string{strings.TrimRight(result.Path, "/")}
	pathParts = append(pathParts, parts...)
	rawPath := strings.Join(pathParts, "/")
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		decodedPath = rawPath
	}
	result.Path = decodedPath
	result.RawPath = rawPath
	return result.String()
}

func parseHTTPURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(value), "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("must be a valid HTTP(S) URL")
	}
	return parsed, nil
}
