package storage

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClient implements the Storage interface
type EtcdClient struct {
	client *clientv3.Client // etcdClient as a struct field
}

// NewEtcdClient creates a new EtcdClient instance
func NewEtcdClient(endpoints []string, timeout time.Duration) (*EtcdClient, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
	})
	if err != nil {
		return nil, err
	}

	return &EtcdClient{client: client}, nil
}

// Close closes the etcd client
func (s *EtcdClient) Close() error {
	if s.client != nil {
		err := s.client.Close() // close the etcd client
		if err != nil {
			return err
		}
		s.client = nil // clear the client reference
	}
	return nil
}

// LoadTimestamp gets the timestamp
func (s *EtcdClient) LoadTimestamp() (time.Time, error) {
	resp, err := s.client.Get(context.Background(), "lastTimestamp") // use the struct field
	if err != nil {
		return time.Time{}, err
	}

	if len(resp.Kvs) > 0 {
		return time.Parse(time.RFC3339, string(resp.Kvs[0].Value))
	}
	return time.Time{}, nil
}

// SaveTimestamp saves the timestamp
func (s *EtcdClient) SaveTimestamp(ts time.Time) error {
	_, err := s.client.Put(context.Background(), "lastTimestamp", ts.Format(time.RFC3339))
	return err
}
