package storage

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClient implements the Storage interface
type EtcdClient struct {
	Client *clientv3.Client
}

// NewEtcdClient creates a new EtcdClient instance
func NewEtcdClient(endpoints []string, timeout time.Duration) (Storage, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("endpoints cannot be empty")
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdClient{Client: client}, nil
}

// Close closes the etcd client
func (s *EtcdClient) Close() error {
	if s.Client != nil {
		err := s.Client.Close()
		s.Client = nil
		return err
	}
	return nil
}

// LoadTimestamp gets the timestamp
func (s *EtcdClient) LoadTimestamp() (time.Time, error) {
	if s.Client == nil {
		return time.Time{}, fmt.Errorf("client is closed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.Client.Get(ctx, "lastTimestamp")
	if err != nil {
		return time.Time{}, err
	}

	if len(resp.Kvs) == 0 {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, string(resp.Kvs[0].Value))
}

// SaveTimestamp saves the timestamp
func (s *EtcdClient) SaveTimestamp(ts time.Time) error {
	if s.Client == nil {
		return fmt.Errorf("client is closed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.Client.Put(ctx, "lastTimestamp", ts.UTC().Format(time.RFC3339))
	return err
}
