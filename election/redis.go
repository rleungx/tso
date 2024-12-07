package election

import (
	"context"
	"fmt"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/rleungx/tso/logger"
	"go.uber.org/zap"
)

const (
	// Election lock key
	electionKey = "/election/active"
	// TTL related constants
	lockTTL         = 1 * time.Second        // Lock expiration time
	renewalInterval = 500 * time.Millisecond // Renewal interval
)

// Election structure
type redisElection struct {
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	client   *redis.Client
	id       string
	fn       func() error
	isActive bool
}

// Create new election instance
func newRedisElection(ctx context.Context, client *redis.Client, id string, fn ...func() error) (Election, error) {
	ctx, cancel := context.WithCancel(ctx)
	e := &redisElection{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		id:     id,
	}

	if len(fn) > 0 {
		e.fn = fn[0]
	}

	// Start election loop
	e.wg.Add(1)
	go e.electionLoop()
	return e, nil
}

// Implement Election interface methods
func (e *redisElection) Campaign() error {
	cctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()

	success, err := e.client.SetNX(cctx, electionKey, e.id, lockTTL).Result()
	if err != nil {
		return fmt.Errorf("failed to set lock: %v", err)
	}

	if !success {
		return fmt.Errorf("active node exists")
	}

	e.wg.Add(1)
	go e.renew()
	return nil
}

func (e *redisElection) Resign() error {
	ctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()
	// Use transaction to ensure only delete your own lock
	txf := func(tx *redis.Tx) error {
		// Check if you are the leader
		val, err := tx.Get(ctx, electionKey).Result()
		if err == redis.Nil {
			return nil // Lock has already been deleted, considered normal
		}
		if err != nil {
			return err
		}

		// Only delete lock if you are the leader
		if val == e.id {
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				return pipe.Del(ctx, electionKey).Err()
			})
		}
		return err
	}

	return e.client.Watch(ctx, txf, electionKey)
}

func (e *redisElection) Close() error {
	e.cancel()

	e.wg.Wait()
	// Finally close the Redis client
	return e.client.Close()
}

func (e *redisElection) IsActive() bool {
	return e.isActive
}

func (e *redisElection) electionLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			logger.Info("election loop context done, exiting")
			return
		default:
			logger.Info("starting election campaign")
			err := e.Campaign()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			e.isActive = true
			logger.Info("successfully became active node")
			if err := e.fn(); err != nil {
				logger.Error("failed to run function, step down", zap.Error(err))
				e.Resign()
				e.isActive = false
				continue
			}
		}
	}
}

func (e *redisElection) renew() {
	defer e.wg.Done()
	ticker := time.NewTicker(renewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			e.isActive = false
			_ = e.Resign()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(e.ctx, time.Second)
			_, err := e.client.Expire(ctx, electionKey, lockTTL).Result()
			cancel()
			if err != nil {
				e.isActive = false
				_ = e.Resign()
				return
			}
		}
	}
}

// SetFn set the function to be executed
func (e *redisElection) SetFn(fn func() error) {
	e.fn = fn
}
