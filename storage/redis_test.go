package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestRedisClient(t *testing.T) {
	// Start miniredis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Create Redis client using miniredis address
	client, err := NewRedisClient(mr.Addr())
	require.NoError(t, err)
	defer client.Close()

	t.Run("SaveAndLoadTimestamp", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Second)

		err := client.SaveTimestamp(now)
		require.NoError(t, err)

		loaded, err := client.LoadTimestamp()
		require.NoError(t, err)
		require.Equal(t, now, loaded)
	})

	t.Run("LoadNonExistentTimestamp", func(t *testing.T) {
		mr.FlushAll() // Clear all data

		ts, err := client.LoadTimestamp()
		require.NoError(t, err)
		require.True(t, ts.IsZero())
	})

	t.Run("HandleConnectionError", func(t *testing.T) {
		// Create client with invalid address
		badClient, err := NewRedisClient("invalid:6379")
		require.NoError(t, err)
		defer badClient.Close()

		// Test save operation failure
		err = badClient.SaveTimestamp(time.Now())
		require.Error(t, err)

		// Test load operation failure
		_, err = badClient.LoadTimestamp()
		require.Error(t, err)
	})

	t.Run("HandleInvalidData", func(t *testing.T) {
		// Set invalid data directly in Redis
		mr.Set("timestamp", "invalid-timestamp")

		// Try to load invalid data
		_, err := client.LoadTimestamp()
		require.Error(t, err)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		timestamp := time.Now().UTC().Truncate(time.Second)
		const goroutines = 10

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				err := client.SaveTimestamp(timestamp)
				require.NoError(t, err)

				loaded, err := client.LoadTimestamp()
				require.NoError(t, err)
				require.Equal(t, timestamp, loaded)
			}()
		}
		wg.Wait()
	})

	t.Run("SaveNilTimestamp", func(t *testing.T) {
		var zeroTime time.Time
		err := client.SaveTimestamp(zeroTime)
		require.NoError(t, err)

		loaded, err := client.LoadTimestamp()
		require.NoError(t, err)
		require.True(t, loaded.IsZero())
	})
}
