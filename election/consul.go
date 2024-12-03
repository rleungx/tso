package election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/rleungx/tso/logger"
)

type consulElection struct {
	client   *api.Client
	key      string
	session  string
	ctx      context.Context
	cancel   context.CancelFunc
	fn       func() error
	mu       sync.Mutex
	isActive bool
}

func newConsulElection(ctx context.Context, client *api.Client, fn func() error) (Election, error) {
	ctx, cancel := context.WithCancel(ctx)
	key := "/election"

	e := &consulElection{
		client:   client,
		key:      key,
		ctx:      ctx,
		cancel:   cancel,
		fn:       fn,
		isActive: false,
	}

	go e.electionLoop()
	return e, nil
}

func (e *consulElection) Campaign(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Create session
	sessionID, _, err := e.client.Session().Create(&api.SessionEntry{
		TTL:       "10s",
		Behavior:  api.SessionBehaviorDelete,
		LockDelay: 0,
	}, nil)
	if err != nil {
		return err
	}
	e.session = sessionID

	kvpair := &api.KVPair{
		Key:     e.key,
		Value:   []byte("leader"),
		Session: e.session,
	}

	acquired, _, err := e.client.KV().Acquire(kvpair, nil)
	if err != nil {
		return err
	}

	if acquired {
		e.isActive = true
		go func() {
			for {
				select {
				case <-e.ctx.Done():
					logger.Info("renew session loop exit")
					e.Resign()
					return
				case <-time.After(3 * time.Second):
					_, _, err := e.client.Session().Renew(e.session, nil)
					if err != nil {
						continue
					}
				}
			}
		}()
		return nil
	}
	return fmt.Errorf("failed to acquire lock")
}

func (e *consulElection) Resign() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.session == "" {
		return fmt.Errorf("no active session to resign")
	}

	_, err := e.client.Session().Destroy(e.session, nil)
	if err != nil {
		return err
	}
	e.isActive = false
	return nil
}

func (e *consulElection) Close() error {
	e.cancel()
	return nil
}

func (e *consulElection) IsActive() bool {
	return e.isActive
}

func (e *consulElection) electionLoop() {
	defer e.cancel()
	for {
		select {
		case <-e.ctx.Done():
			return
		default:
			if err := e.Campaign(e.ctx); err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			logger.Info("become active")
			// Execute the specified function
			if err := e.fn(); err != nil {
				e.Resign() // Resign on failure
				continue
			}
		}
	}
}
