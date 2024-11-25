package election

import (
	"context"
	"fmt"

	"github.com/rleungx/tso/storage"
)

// Election defines the election interface
type Election interface {
	// Watch listens for election state changes
	Watch(ctx context.Context)
	Campaign(ctx context.Context) error
	// Resign voluntarily gives up the election qualification
	Resign() error
	// Close closes the election
	Close() error
}

// NewElection creates an election instance
func NewElection(ctx context.Context, s storage.Storage) (Election, error) {
	// Use type switch statement for type checking
	switch v := s.(type) {
	case *storage.EtcdClient:
		return newEtcdElection(ctx, v.Client)
	case *storage.MemStorage:
		return newMemElection()
	default:
		return nil, fmt.Errorf("unsupported storage type: %T", s)
	}
}
