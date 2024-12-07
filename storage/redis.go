package storage

import (
	"context"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

// NewRedisClient creates a new Redis client
func NewRedisClient(addr string) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisClient{Client: client}, nil
}

// Close closes the Redis client connection
func (r *RedisClient) Close() error {
	return r.Client.Close()
}

// SaveTimestamp saves a timestamp to Redis
func (r *RedisClient) SaveTimestamp(ts time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := r.Client.Set(ctx, "timestamp", ts.Format(time.RFC3339), 0).Result()
	return err
}

// LoadTimestamp loads a timestamp from Redis
func (r *RedisClient) LoadTimestamp() (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	val, err := r.Client.Get(ctx, "timestamp").Result()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	ts, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, err
	}
	return ts, nil
}
