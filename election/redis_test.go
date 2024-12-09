package election

import (
	"context"
	"fmt"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, func()) {
	// Start an embedded Redis server
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	// Return a cleanup function to close miniredis
	return mr, func() {
		mr.Close()
	}
}

func TestRedisElection(t *testing.T) {
	t.Run("basic election process", func(t *testing.T) {
		mr, cleanup := setupTestRedis(t)
		defer cleanup()
		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel() // Ensure context is cancelled when test ends
		called := false

		// Create the first node
		client1 := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		e1, err := newRedisElection(ctx, client1, "node1")
		require.NoError(t, err)
		e1.(*redisElection).setFn(func() error {
			called = true
			<-e1.(*redisElection).ctx.Done()
			return nil
		})
		defer e1.Close()

		// Wait for the election to complete
		time.Sleep(100 * time.Millisecond)
		require.True(t, e1.IsActive())
		require.True(t, called)

		// Try to create the second node
		called = false
		client2 := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		e2, err := newRedisElection(ctx, client2, "node2")
		require.NoError(t, err)
		e2.(*redisElection).setFn(func() error {
			called = true
			<-e2.(*redisElection).ctx.Done()
			return nil
		})
		defer e2.Close()

		// The second node should not be active
		time.Sleep(100 * time.Millisecond)
		require.False(t, e2.IsActive())
		require.False(t, called)
	})

	t.Run("re-election after node exit", func(t *testing.T) {
		mr, cleanup := setupTestRedis(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel() // Ensure context is cancelled when test ends

		client1 := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		e1, err := newRedisElection(ctx, client1, "node1")
		require.NoError(t, err)
		e1.(*redisElection).setFn(func() error {
			<-e1.(*redisElection).ctx.Done()
			return nil
		})

		client2 := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		e2, err := newRedisElection(ctx, client2, "node2")
		require.NoError(t, err)
		e2.(*redisElection).setFn(func() error {
			<-e2.(*redisElection).ctx.Done()
			return nil
		})
		defer e2.Close()

		// Close the first node
		e1.Close()

		// Manually advance miniredis time to expire TTL
		mr.FastForward(100 * time.Millisecond)

		// Wait for a short period for node2 to acquire the lock
		time.Sleep(100 * time.Millisecond)

		// The second node should become active
		require.True(t, e2.IsActive())
	})

	t.Run("test function execution failure", func(t *testing.T) {
		mr, cleanup := setupTestRedis(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		defer client.Close()

		failCount := 0
		e, err := newRedisElection(ctx, client, "node-fail")
		require.NoError(t, err)
		e.(*redisElection).setFn(func() error {
			failCount++
			return fmt.Errorf("simulated execution failure")
		})
		defer e.Close()

		// Increase wait time to ensure state update
		time.Sleep(2 * time.Second)
		require.False(t, e.IsActive())
		require.Greater(t, failCount, 0)
	})

	t.Run("test renewal failure scenario", func(t *testing.T) {
		mr, cleanup := setupTestRedis(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})
		defer client.Close()

		e, err := newRedisElection(ctx, client, "node-renew")
		require.NoError(t, err)
		e.(*redisElection).setFn(func() error {
			<-e.(*redisElection).ctx.Done()
			return nil
		})
		defer e.Close()

		// Wait for the node to become active
		time.Sleep(100 * time.Millisecond)
		require.True(t, e.IsActive())

		// Simulate an exception by closing Redis service
		mr.Close()

		// Wait for a duration longer than TTL
		mr.FastForward(1200 * time.Millisecond)

		// Quickly check node status
		success := false
		for i := 0; i < 5; i++ {
			time.Sleep(100 * time.Millisecond)
			if !e.IsActive() {
				success = true
				break
			}
		}
		require.True(t, success, "node should become inactive at some point")
	})

	t.Run("test multi-node competition", func(t *testing.T) {
		mr, cleanup := setupTestRedis(t)
		defer cleanup()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create multiple nodes
		nodeCount := 5
		nodes := make([]Election, nodeCount)
		activeCount := 0

		for i := 0; i < nodeCount; i++ {
			client := redis.NewClient(&redis.Options{
				Addr: mr.Addr(),
			})
			e, err := newRedisElection(ctx, client, fmt.Sprintf("node-%d", i))
			require.NoError(t, err)
			e.(*redisElection).setFn(func() error {
				<-e.(*redisElection).ctx.Done()
				return nil
			})
			nodes[i] = e
			defer e.Close()
		}

		// Wait for the election to complete
		time.Sleep(200 * time.Millisecond)

		// Check that only one node is active
		for _, node := range nodes {
			if node.IsActive() {
				activeCount++
			}
		}
		require.Equal(t, 1, activeCount)
	})
}
