package storage

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

// EtcdClient implements the Storage interface
type ConsulClient struct {
	Client *api.Client
}

// NewConsulClient creates a new ConsulClient instance
func NewConsulClient(addr string) (*ConsulClient, error) {
	config := api.DefaultConfig()
	config.Address = addr
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &ConsulClient{
		Client: client,
	}, nil
}

// Close closes the etcd client
func (s *ConsulClient) Close() error {
	return nil
}

// LoadTimestamp gets the timestamp
func (s *ConsulClient) LoadTimestamp() (time.Time, error) {
	if s.Client == nil {
		return time.Time{}, fmt.Errorf("client is closed")
	}

	kvPair, _, err := s.Client.KV().Get("lastTimestamp", nil)
	if err != nil {
		return time.Time{}, err
	}
	if kvPair == nil {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, string(kvPair.Value))
}

// SaveTimestamp saves the timestamp
func (s *ConsulClient) SaveTimestamp(ts time.Time) error {
	if s.Client == nil {
		return fmt.Errorf("client is closed")
	}

	kvPair := api.KVPair{
		Key:   "lastTimestamp",
		Value: []byte(ts.UTC().Format(time.RFC3339)),
	}
	_, err := s.Client.KV().Put(&kvPair, nil)
	return err
}
