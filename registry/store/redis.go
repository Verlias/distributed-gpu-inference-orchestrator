package store

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/internal/resp"
	"github.com/Verlias/distributed-gpu-inference-orchestrator/registry/models"
)

const (
	defaultRedisAddress = "localhost:6379"
	defaultKeyPrefix    = "gpu-orchestrator"
)

// RedisConfig configures the Redis-backed worker store.
type RedisConfig struct {
	Address     string
	Password    string
	Database    int
	KeyPrefix   string
	DialTimeout time.Duration
	IOTimeout   time.Duration
}

// RedisStore persists workers as Redis hashes and maintains an ID index set.
type RedisStore struct {
	client *redisClient
	prefix string
}

func NewRedis(config RedisConfig) *RedisStore {
	if strings.TrimSpace(config.Address) == "" {
		config.Address = defaultRedisAddress
	}
	if strings.TrimSpace(config.KeyPrefix) == "" {
		config.KeyPrefix = defaultKeyPrefix
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = 5 * time.Second
	}
	if config.IOTimeout <= 0 {
		config.IOTimeout = 5 * time.Second
	}
	return &RedisStore{
		client: &redisClient{
			address: config.Address, password: config.Password, database: config.Database,
			dialTimeout: config.DialTimeout, ioTimeout: config.IOTimeout,
		},
		prefix: strings.TrimRight(config.KeyPrefix, ":"),
	}
}

func (s *RedisStore) Ping(ctx context.Context) error {
	_, err := s.client.do(ctx, "PING")
	return err
}

func (s *RedisStore) Create(ctx context.Context, worker models.Worker, leaseTTL time.Duration) (models.Worker, error) {
	const script = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'worker_id', ARGV[1], 'endpoint', ARGV[2], 'health_status', ARGV[3],
  'last_heartbeat', '', 'lease_expires_at', ARGV[4],
  'waiting_requests', '0', 'kv_cache_utilization', '0')
redis.call('PEXPIRE', KEYS[1], ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
return 1`
	result, err := s.client.do(ctx, "EVAL", script, "2", s.workerKey(worker.ID), s.leaseKey(),
		worker.ID, worker.Endpoint, string(worker.HealthStatus), worker.LeaseExpiresAt.Format(time.RFC3339Nano),
		strconv.FormatInt(leaseTTL.Milliseconds(), 10), strconv.FormatInt(worker.LeaseExpiresAt.UnixMilli(), 10))
	if err != nil {
		return models.Worker{}, fmt.Errorf("create worker in Redis: %w", err)
	}
	created, err := integerResult(result)
	if err != nil {
		return models.Worker{}, err
	}
	if created == 0 {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerExists, worker.ID)
	}
	return s.Get(ctx, worker.ID)
}

func (s *RedisStore) UpsertIdentity(
	ctx context.Context, input models.WorkerRegistration, leaseExpiresAt time.Time, leaseTTL time.Duration,
) (models.Worker, error) {
	const script = `
redis.call('HSET', KEYS[1], 'worker_id', ARGV[1], 'endpoint', ARGV[2])
redis.call('HSETNX', KEYS[1], 'health_status', ARGV[3])
redis.call('HSETNX', KEYS[1], 'last_heartbeat', '')
redis.call('HSETNX', KEYS[1], 'waiting_requests', '0')
redis.call('HSETNX', KEYS[1], 'kv_cache_utilization', '0')
redis.call('HSET', KEYS[1], 'lease_expires_at', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
return 1`
	_, err := s.client.do(ctx, "EVAL", script, "2", s.workerKey(input.ID), s.leaseKey(),
		input.ID, input.Endpoint, string(models.HealthStatusUnknown), leaseExpiresAt.Format(time.RFC3339Nano),
		strconv.FormatInt(leaseTTL.Milliseconds(), 10), strconv.FormatInt(leaseExpiresAt.UnixMilli(), 10))
	if err != nil {
		return models.Worker{}, fmt.Errorf("upsert worker in Redis: %w", err)
	}
	return s.Get(ctx, input.ID)
}

func (s *RedisStore) UpdateHeartbeat(
	ctx context.Context, id string, heartbeat models.Heartbeat, receivedAt, leaseExpiresAt time.Time, leaseTTL time.Duration,
) (models.Worker, error) {
	const script = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1],
  'health_status', ARGV[1], 'last_heartbeat', ARGV[2],
  'waiting_requests', ARGV[3], 'kv_cache_utilization', ARGV[4],
  'lease_expires_at', ARGV[5])
redis.call('PEXPIRE', KEYS[1], ARGV[6])
redis.call('ZADD', KEYS[2], ARGV[7], ARGV[8])
return 1`
	result, err := s.client.do(ctx, "EVAL", script, "2", s.workerKey(id), s.leaseKey(),
		string(heartbeat.HealthStatus), receivedAt.UTC().Format(time.RFC3339Nano),
		strconv.Itoa(heartbeat.WaitingRequests), strconv.FormatFloat(heartbeat.KVCacheUtilization, 'g', -1, 64),
		leaseExpiresAt.Format(time.RFC3339Nano), strconv.FormatInt(leaseTTL.Milliseconds(), 10),
		strconv.FormatInt(leaseExpiresAt.UnixMilli(), 10), id)
	if err != nil {
		return models.Worker{}, fmt.Errorf("update worker heartbeat in Redis: %w", err)
	}
	updated, err := integerResult(result)
	if err != nil {
		return models.Worker{}, err
	}
	if updated == 0 {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	return s.Get(ctx, id)
}

func (s *RedisStore) Get(ctx context.Context, id string) (models.Worker, error) {
	result, err := s.client.do(ctx, "HGETALL", s.workerKey(id))
	if err != nil {
		return models.Worker{}, fmt.Errorf("get worker from Redis: %w", err)
	}
	fields, err := stringMapResult(result)
	if err != nil {
		return models.Worker{}, err
	}
	if len(fields) == 0 {
		return models.Worker{}, fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	return decodeWorker(fields)
}

func (s *RedisStore) List(ctx context.Context, now time.Time) ([]models.Worker, error) {
	result, err := s.client.do(ctx, "ZRANGEBYSCORE", s.leaseKey(),
		"("+strconv.FormatInt(now.UnixMilli(), 10), "+inf")
	if err != nil {
		return nil, fmt.Errorf("list worker IDs from Redis: %w", err)
	}
	ids, err := stringSliceResult(result)
	if err != nil {
		return nil, err
	}
	workers := make([]models.Worker, 0, len(ids))
	for _, id := range ids {
		worker, getErr := s.Get(ctx, id)
		if errors.Is(getErr, ErrWorkerNotFound) {
			continue
		}
		if getErr != nil {
			return nil, getErr
		}
		workers = append(workers, worker)
	}
	return workers, nil
}

func (s *RedisStore) Delete(ctx context.Context, id string) error {
	const script = `
local deleted = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return deleted`
	result, err := s.client.do(ctx, "EVAL", script, "2", s.workerKey(id), s.leaseKey(), id)
	if err != nil {
		return fmt.Errorf("delete worker from Redis: %w", err)
	}
	deleted, err := integerResult(result)
	if err != nil {
		return err
	}
	if deleted == 0 {
		return fmt.Errorf("%w: %s", ErrWorkerNotFound, id)
	}
	return nil
}

func (s *RedisStore) ReapExpired(ctx context.Context, now time.Time) error {
	_, err := s.client.do(ctx, "ZREMRANGEBYSCORE", s.leaseKey(), "-inf",
		strconv.FormatInt(now.UnixMilli(), 10))
	if err != nil {
		return fmt.Errorf("reap expired worker leases from Redis: %w", err)
	}
	return nil
}

// Close is a no-op because the dependency-free Redis client opens a bounded
// connection per operation instead of retaining a connection pool.
func (s *RedisStore) Close() error { return nil }

func (s *RedisStore) leaseKey() string { return s.prefix + ":v2:leases" }

func (s *RedisStore) workerKey(id string) string {
	encodedID := base64.RawURLEncoding.EncodeToString([]byte(id))
	return s.prefix + ":v2:worker:" + encodedID
}

func decodeWorker(fields map[string]string) (models.Worker, error) {
	waitingRequests, err := strconv.Atoi(fields["waiting_requests"])
	if err != nil {
		return models.Worker{}, fmt.Errorf("decode waiting_requests from Redis: %w", err)
	}
	kvUtilization, err := strconv.ParseFloat(fields["kv_cache_utilization"], 64)
	if err != nil {
		return models.Worker{}, fmt.Errorf("decode kv_cache_utilization from Redis: %w", err)
	}
	worker := models.Worker{
		ID: fields["worker_id"], Endpoint: fields["endpoint"],
		HealthStatus:    models.HealthStatus(fields["health_status"]),
		WaitingRequests: waitingRequests, KVCacheUtilization: kvUtilization,
	}
	leaseExpiresAt, err := time.Parse(time.RFC3339Nano, fields["lease_expires_at"])
	if err != nil {
		return models.Worker{}, fmt.Errorf("decode lease_expires_at from Redis: %w", err)
	}
	worker.LeaseExpiresAt = leaseExpiresAt
	if value := fields["last_heartbeat"]; value != "" {
		lastHeartbeat, parseErr := time.Parse(time.RFC3339Nano, value)
		if parseErr != nil {
			return models.Worker{}, fmt.Errorf("decode last_heartbeat from Redis: %w", parseErr)
		}
		worker.LastHeartbeat = &lastHeartbeat
	}
	return worker, nil
}

type redisClient struct {
	address     string
	password    string
	database    int
	dialTimeout time.Duration
	ioTimeout   time.Duration
}

func (c *redisClient) do(ctx context.Context, arguments ...string) (any, error) {
	dialer := net.Dialer{Timeout: c.dialTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", c.address)
	if err != nil {
		return nil, fmt.Errorf("connect to Redis at %s: %w", c.address, err)
	}
	defer connection.Close()
	deadline := time.Now().Add(c.ioTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)
	if c.password != "" {
		if _, err := executeRedisCommand(reader, writer, "AUTH", c.password); err != nil {
			return nil, fmt.Errorf("authenticate with Redis: %w", err)
		}
	}
	if c.database != 0 {
		if _, err := executeRedisCommand(reader, writer, "SELECT", strconv.Itoa(c.database)); err != nil {
			return nil, fmt.Errorf("select Redis database: %w", err)
		}
	}
	return executeRedisCommand(reader, writer, arguments...)
}

func executeRedisCommand(reader *bufio.Reader, writer *bufio.Writer, arguments ...string) (any, error) {
	if err := resp.WriteCommand(writer, arguments...); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return resp.ReadResponse(reader)
}

func integerResult(value any) (int64, error) {
	result, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("expected Redis integer response, got %T", value)
	}
	return result, nil
}

func stringSliceResult(value any) ([]string, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected Redis array response, got %T", value)
	}
	result := make([]string, len(values))
	for index, item := range values {
		stringValue, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("expected Redis string at index %d, got %T", index, item)
		}
		result[index] = stringValue
	}
	return result, nil
}

func stringMapResult(value any) (map[string]string, error) {
	values, err := stringSliceResult(value)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("Redis hash response contains an odd number of values")
	}
	result := make(map[string]string, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		result[values[index]] = values[index+1]
	}
	return result, nil
}
