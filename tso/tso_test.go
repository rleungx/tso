package tso

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rleungx/tso/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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
				err := tso.syncTimestamp(storage)
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
			name: "Test comprehensive clock rollback scenarios",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()

				// 1. Test direct clock rollback
				oldPhysical := tso.physical
				tso.setPhysical(oldPhysical.Add(-500*time.Millisecond), false)
				require.Equal(t, oldPhysical, tso.physical, "Physical clock should not regress")

				// 2. Test clock rollback through storage
				physical1, _, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)

				backwardTime := time.Now().Add(-time.Second)
				err = storage.SaveTimestamp(backwardTime)
				require.NoError(t, err)

				err = tso.syncTimestamp(storage)
				require.NoError(t, err)

				physical2, _, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)
				require.GreaterOrEqual(t, physical2, physical1, "Physical time should not decrease after rollback")
			},
		},
		{
			name: "Test consecutive timestamp generation",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()
				var lastPhysical, lastLogical int64
				timestamps := make([]int64, 0)

				// Test slow and fast consecutive timestamp generation
				for i := 0; i < 100; i++ {
					physical, logical, err := tso.GenerateTimestamp(ctx, 1)
					require.NoError(t, err)

					if i > 0 {
						require.GreaterOrEqual(t, physical, lastPhysical, "physical time regressed")
						if physical == lastPhysical {
							require.Greater(t, logical, lastLogical, "logical did not increase")
						}
					}

					lastPhysical, lastLogical = physical, logical
					timestamps = append(timestamps, physical<<18+logical)

					if i < 5 {
						continue
					}
					time.Sleep(time.Millisecond)
				}

				// Verify timestamp uniqueness
				seen := make(map[int64]bool)
				for _, ts := range timestamps {
					require.False(t, seen[ts], "Duplicate timestamp found")
					seen[ts] = true
				}
			},
		},
		{
			name: "Test boundary conditions for logical clock",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				tso.setPhysical(time.Now(), true)
				tso.logical = maxLogical - 3
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()

				// Test behavior when logical clock approaches maximum value
				physical1, logical1, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)

				physical2, logical2, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)

				// Verify timestamp continuity
				if physical1 == physical2 {
					require.Equal(t, logical1+1, logical2)
				} else {
					require.Greater(t, physical2, physical1)
					require.Equal(t, int64(0), logical2)
				}
			},
		},
		{
			name: "Test physical time update with update interval",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()

				// Record initial physical time
				physical1, logical1, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)

				// Wait for less than updateInterval
				time.Sleep(time.Millisecond)

				// Physical time should remain constant, only logical time should increase
				physical2, logical2, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)
				require.Equal(t, physical1, physical2, "Physical time should remain constant, only logical time should increase")
				require.Greater(t, logical2, logical1, "Logical time should increase")
			},
		},
		{
			name: "Test timestamp with clock moving backwards",
			setupFunc: func(tso *TimestampOracle, storage storage.Storage) {
				now := time.Now()
				tso.setPhysical(now, true)
			},
			testFunc: func(t *testing.T, tso *TimestampOracle, storage storage.Storage) {
				ctx := context.Background()

				// Get current timestamp
				physical1, _, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)

				// Simulate clock rollback
				backwardTime := time.Now().Add(-time.Second)
				tso.setPhysical(backwardTime, false)

				// Even with clock moving backwards, new timestamp should not be less than previous timestamp
				physical2, _, err := tso.GenerateTimestamp(ctx, 1)
				require.NoError(t, err)
				require.GreaterOrEqual(t, physical2, physical1, "Physical time should not decrease during clock rollback")
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
			err := tso.syncTimestamp(storage)
			if (err != nil) != tt.wantErr {
				t.Errorf("syncTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUpdateTimestamp(t *testing.T) {
	// Create a new MemStorage instance
	memStorage, err := storage.NewMemStorage()
	require.NoError(t, err)
	defer memStorage.Close()

	// Create a new TimestampOracle instance
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tso := NewTimestampOracle(ctx, memStorage)
	// Initialize timestamp
	err = tso.syncTimestamp(memStorage)
	require.NoError(t, err, "syncTimestamp failed")

	// Manually set logical clock close to maximum value
	tso.logical = maxLogical - 1
	// Call updateTimestamp method
	err = tso.updateTimestamp(memStorage)
	require.NoError(t, err, "updateTimestamp failed")
	// Get updated timestamp
	newPhysical, newLogical := tso.get()
	// Check if logical time has not overflowed
	require.Equal(t, int64(0), newLogical, "expected logical time to be reset to 0 after update")
	// Check if physical time has been updated
	require.NotEqual(t, ZeroTime, newPhysical, "expected physical time to be updated")
}
