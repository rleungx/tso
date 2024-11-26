package tso

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rleungx/tso/storage"
	"github.com/stretchr/testify/require"
)

func TestTimestampOracle(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*TimestampOracle, storage.Storage)
		testFunc  func(*testing.T, *TimestampOracle, storage.Storage)
	}{
		{
			name: "Generate consecutive timestamps - verify monotonic increase",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()
				var lastPhysical, lastLogical int64

				for i := 0; i < 100; i++ {
					physical, logical, err := tso.GenerateTimestamp(ctx, 1)
					require.NoError(t, err, "Failed to generate timestamp")

					if i > 0 {
						require.GreaterOrEqual(t, physical, lastPhysical, "physical time regressed")
						if physical == lastPhysical {
							require.Greater(t, logical, lastLogical, "logical did not increase")
						}
					}

					lastPhysical, lastLogical = physical, logical
					time.Sleep(time.Millisecond)
				}
			},
		},
		{
			name: "Generate timestamps concurrently - verify thread safety",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				tso.setPhysical(time.Now(), true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				var wg sync.WaitGroup
				ctx := context.Background()
				results := make(chan struct {
					physical int64
					logical  int64
				}, 1000)

				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < 100; j++ {
							physical, logical, err := tso.GenerateTimestamp(ctx, 1)
							require.NoError(t, err, "Failed to generate timestamp")
							results <- struct {
								physical int64
								logical  int64
							}{physical, logical}
						}
					}()
				}

				go func() {
					wg.Wait()
					close(results)
				}()

				seen := make(map[string]bool)
				for result := range results {
					key := fmt.Sprintf("%d-%d", result.physical, result.logical)
					require.False(t, seen[key], "Detected duplicate timestamp: %s", key)
					seen[key] = true
				}
			},
		},
		{
			name: "Test logical clock exhaustion scenario",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
				tso.logical = maxLogical - 10
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()

				// Sync timestamp first
				err := tso.SyncTimestamp(storage)
				require.NoError(t, err, "Failed to sync timestamp")

				// Test normal allocation
				_, logical, err := tso.GenerateTimestamp(ctx, 5)
				require.NoError(t, err, "Failed to generate timestamp")
				require.LessOrEqual(t, logical, maxLogical, "Logical clock exceeded maximum value")

				// Test allocation exceeding maximum value
				_, _, err = tso.GenerateTimestamp(ctx, 10)
				require.ErrorIs(t, err, ErrLogicalOverflow, "Should return logical clock overflow error")
			},
		},
		{
			name: "Test clock rollback scenario",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now.Add(time.Second), true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				oldPhysical := tso.physical
				tso.setPhysical(oldPhysical.Add(-500*time.Millisecond), false)

				require.Equal(t, oldPhysical, tso.physical, "Physical clock regressed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			storage, err := storage.NewMemStorage()
			require.NoError(t, err, "Failed to create storage")
			tso := NewTimestampOracle(ctx, storage)

			if tt.setupFunc != nil {
				tt.setupFunc(tso, storage)
			}

			tt.testFunc(t, tso, storage)
		})
	}
}

func TestSyncTimestamp(t *testing.T) {
	ctx := context.Background()
	storage, err := storage.NewMemStorage()
	require.NoError(t, err, "Failed to create storage")
	tso := NewTimestampOracle(ctx, storage)

	tests := []struct {
		name    string
		setup   func()
		wantErr bool
	}{
		{
			name: "Normal sync - interval greater than protection value",
			setup: func() {
				storage.SaveTimestamp(time.Now().Add(-time.Second))
			},
			wantErr: false,
		},
		{
			name: "Normal sync - interval less than protection value",
			setup: func() {
				storage.SaveTimestamp(time.Now().Add(-time.Microsecond))
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			err := tso.SyncTimestamp(storage)
			if (err != nil) != tt.wantErr {
				t.Errorf("SyncTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
