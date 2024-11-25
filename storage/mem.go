package storage

import (
	"errors"
	"sync"
	"time"
)

// MemStorage is an in-memory implementation of the Storage interface
type MemStorage struct {
	data   map[string]interface{}
	mu     sync.RWMutex
	closed bool
}

// NewMemStorage creates a new instance of MemStorage
func NewMemStorage() (Storage, error) {
	return &MemStorage{
		data: make(map[string]interface{}),
	}, nil
}

// Close closes the storage
func (m *MemStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("storage is already closed")
	}
	m.closed = true
	return nil
}

// SaveTimestamp stores a timestamp
func (m *MemStorage) SaveTimestamp(ts time.Time) error {
	return m.set("timestamp", ts)
}

// LoadTimestamp loads the stored timestamp
func (m *MemStorage) LoadTimestamp() (time.Time, error) {
	value, err := m.get("timestamp")
	if err != nil {
		return time.Time{}, err
	}

	if value == nil {
		return time.Time{}, nil
	}

	ts, ok := value.(time.Time)
	if !ok {
		return time.Time{}, errors.New("stored value is not a time.Time")
	}
	return ts, nil
}

// set stores a key-value pair
func (m *MemStorage) set(key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("storage is closed")
	}
	m.data[key] = value
	return nil
}

// get retrieves the value corresponding to a key
func (m *MemStorage) get(key string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, errors.New("storage is closed")
	}
	return m.data[key], nil
}
