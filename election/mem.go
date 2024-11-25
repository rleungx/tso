package election

import (
	"context"
)

// MemElection is an in-memory implementation for testing
type MemElection struct {
}

func newMemElection() (Election, error) {
	return &MemElection{}, nil
}

func (e *MemElection) Watch(ctx context.Context) {
}

func (e *MemElection) Close() error {
	return nil
}

func (m *MemElection) Resign() error {
	return nil
}

func (m *MemElection) Campaign(ctx context.Context) error {
	return nil
}
