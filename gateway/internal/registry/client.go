// Package registry implements the gateway's worker-registry HTTP client.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("worker registry unavailable")

type Worker struct {
	ID       string `json:"worker_id"`
	Endpoint string `json:"endpoint"`
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid registry URL %q", baseURL)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: time.Second}
	}
	return &Client{baseURL: parsed, httpClient: httpClient}, nil
}

func (c *Client) Select(ctx context.Context) (Worker, error) {
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/workers/select"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return Worker{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return Worker{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return Worker{}, fmt.Errorf("%w: registry returned %s: %s",
			ErrUnavailable, response.Status, strings.TrimSpace(string(body)))
	}
	var worker Worker
	if err := json.NewDecoder(response.Body).Decode(&worker); err != nil {
		return Worker{}, fmt.Errorf("%w: decode selected worker: %v", ErrUnavailable, err)
	}
	if strings.TrimSpace(worker.ID) == "" || strings.TrimSpace(worker.Endpoint) == "" {
		return Worker{}, fmt.Errorf("%w: registry returned an incomplete worker", ErrUnavailable)
	}
	return worker, nil
}
