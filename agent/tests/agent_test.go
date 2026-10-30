package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	monitor "github.com/Verlias/distributed-gpu-inference-orchestrator/agent"
)

type heartbeat struct {
	HealthStatus       string  `json:"health_status"`
	WaitingRequests    int     `json:"waiting_requests"`
	KVCacheUtilization float64 `json:"kv_cache_utilization"`
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestAgentRegistersAndReportsMetrics(t *testing.T) {
	var registered bool
	var received heartbeat
	client := testClient(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Host == "vllm.test" && request.URL.Path == "/health":
			return response(http.StatusOK, ""), nil
		case request.URL.Host == "vllm.test" && request.URL.Path == "/metrics":
			return response(http.StatusOK, `# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0"} 2
vllm:num_requests_waiting{engine="1"} 3
vllm:kv_cache_usage_perc{engine="0"} 0.4
vllm:kv_cache_usage_perc{engine="1"} 0.7
`), nil
		case request.Method == http.MethodPut && request.URL.Path == "/workers/worker-1":
			registered = true
			return response(http.StatusOK, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/workers/worker-1/heartbeat":
			if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
				t.Fatalf("decode heartbeat: %v", err)
			}
			return response(http.StatusOK, `{}`), nil
		default:
			return response(http.StatusNotFound, ""), nil
		}
	})

	agent := newAgent(t, client, time.Second)
	if err := agent.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := agent.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if !registered {
		t.Fatal("expected worker registration")
	}
	if received.HealthStatus != "healthy" || received.WaitingRequests != 5 || received.KVCacheUtilization != 0.7 {
		t.Fatalf("unexpected heartbeat: %#v", received)
	}
}

func TestAgentReportsUnhealthyWhenVLLMFails(t *testing.T) {
	var received heartbeat
	client := testClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "vllm.test" {
			return response(http.StatusServiceUnavailable, "not ready"), nil
		}
		_ = json.NewDecoder(request.Body).Decode(&received)
		return response(http.StatusOK, `{}`), nil
	})
	agent := newAgent(t, client, time.Second)
	if err := agent.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if received.HealthStatus != "unhealthy" || received.WaitingRequests != 0 || received.KVCacheUtilization != 0 {
		t.Fatalf("unexpected heartbeat: %#v", received)
	}
}

func TestAgentReregistersAfterMissingWorker(t *testing.T) {
	var mu sync.Mutex
	registrations := 0
	heartbeats := 0
	client := testClient(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "vllm.test" {
			if request.URL.Path == "/metrics" {
				return response(http.StatusOK, "vllm:num_requests_waiting 0\nvllm:kv_cache_usage_perc 0\n"), nil
			}
			return response(http.StatusOK, ""), nil
		}
		mu.Lock()
		defer mu.Unlock()
		if request.Method == http.MethodPut {
			registrations++
			return response(http.StatusOK, `{}`), nil
		}
		heartbeats++
		if heartbeats == 1 {
			return response(http.StatusNotFound, ""), nil
		}
		return response(http.StatusOK, `{}`), nil
	})

	agent := newAgent(t, client, 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = agent.Run(ctx)
	mu.Lock()
	defer mu.Unlock()
	if registrations < 2 || heartbeats < 2 {
		t.Fatalf("expected re-registration, registrations=%d heartbeats=%d", registrations, heartbeats)
	}
}

func newAgent(t *testing.T, client *http.Client, interval time.Duration) *monitor.Agent {
	t.Helper()
	agent, err := monitor.New(monitor.Config{
		WorkerID: "worker-1", VLLMEndpoint: "http://vllm.test", RegistryURL: "http://registry.test",
		PollInterval: interval,
	}, client)
	if err != nil {
		t.Fatalf("create agent: %v", err)
	}
	return agent
}

func testClient(transport roundTripFunc) *http.Client {
	return &http.Client{Transport: transport, Timeout: time.Second}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)),
	}
}
