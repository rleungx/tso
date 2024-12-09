package election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/rleungx/tso/logger"
	"go.uber.org/zap"
)

type consulElection struct {
	wg       sync.WaitGroup
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	id       string
	client   *api.Client
	key      string
	session  string
	fn       func() error
	isActive bool
}

func newConsulElection(ctx context.Context, client *api.Client, id string, fn ...func() error) (Election, error) {
	ctx, cancel := context.WithCancel(ctx)

	e := &consulElection{
		id:     id,
		client: client,
		key:    "election/active",
		ctx:    ctx,
		cancel: cancel,
	}

	if len(fn) > 0 {
		e.fn = fn[0]
	}
	e.wg.Add(1)
	go e.electionLoop()
	return e, nil
}

func (e *consulElection) Campaign() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Create session
	sessionID, _, err := e.client.Session().Create(&api.SessionEntry{
		TTL:       "10s",
		Behavior:  api.SessionBehaviorDelete,
		LockDelay: time.Millisecond,
	}, nil)
	if err != nil {
		return err
	}
	e.session = sessionID

	kvpair := &api.KVPair{
		Key:     e.key,
		Value:   []byte(e.id),
		Session: e.session,
	}

	acquired, _, err := e.client.KV().Acquire(kvpair, nil)
	if err != nil {
		return err
	}

	if acquired {
		e.isActive = true
		e.wg.Add(1)
		go e.renew()
		return nil
	}
	return fmt.Errorf("failed to acquire lock")
}

func (e *consulElection) renew() {
	defer e.wg.Done()
	for {
		select {
		case <-e.ctx.Done():
			logger.Info("renew session loop exit")
			e.isActive = false
			e.Resign()
			return
		case <-time.After(3 * time.Second):
			_, _, err := e.client.Session().Renew(e.session, nil)
			if err != nil {
				e.isActive = false
				e.Resign()
				return
			}
		}
	}
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
	return nil
}

func (e *consulElection) Close() error {
	e.cancel()
	e.wg.Wait()
	return nil
}

func (e *consulElection) IsActive() bool {
	return e.isActive
}

func (e *consulElection) electionLoop() {
	defer e.wg.Done()
	defer e.cancel()
	for {
		select {
		case <-e.ctx.Done():
			logger.Info("election loop context done, exiting")
			return
		default:
			err := e.Campaign()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			e.isActive = true
			logger.Info("successfully became active node")
			// Execute the specified function
			if err := e.fn(); err != nil {
				e.Resign() // Resign on failure
				e.isActive = false
				logger.Error("failed to run function, step down", zap.Error(err))
				continue
			}
		}
	}
}

// setFn set the function to be executed
func (e *consulElection) setFn(fn func() error) {
	e.fn = fn
}
