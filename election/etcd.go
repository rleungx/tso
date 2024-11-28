package election

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rleungx/tso/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	defaultElectionPrefix = "/election"
)

// Role represents the role
type Role int32

const (
	// RoleStandby indicates standby status, not providing service
	RoleStandby Role = iota
	// RoleActive indicates active status, providing service
	RoleActive
)

func (r Role) String() string {
	switch r {
	case RoleStandby:
		return "Standby"
	case RoleActive:
		return "Active"
	default:
		return "Unknown"
	}
}

type etcdElection struct {
	sync.WaitGroup
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	ctx      context.Context
	cancel   context.CancelFunc
	fn       func() error
	role     atomic.Int32
}

func newEtcdElection(ctx context.Context, client *clientv3.Client, fn func() error) (Election, error) {
	ctx, cancel := context.WithCancel(ctx)
	session, err := concurrency.NewSession(client, concurrency.WithTTL(3))
	if err != nil {
		cancel()
		return nil, err
	}

	// Create election with default path
	election := concurrency.NewElection(session, defaultElectionPrefix)

	e := &etcdElection{
		client:   client,
		session:  session,
		election: election,
		ctx:      ctx,
		cancel:   cancel,
		fn:       fn,
	}
	e.Add(1)
	go e.electionLoop()
	return e, nil
}

func (e *etcdElection) Resign() error {
	if e.election == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return e.election.Resign(ctx)
}

func (e *etcdElection) Close() error {
	e.cancel()
	if e.session != nil {
		return e.session.Close()
	}
	e.Wait()
	return nil
}

func (e *etcdElection) Campaign(ctx context.Context) error {
	// Check if election instance exists
	if e.election == nil {
		return fmt.Errorf("election instance is nil")
	}

	// Participate in election - use empty string as value
	return e.election.Campaign(ctx, "")
}

func (e *etcdElection) electionLoop() {
	defer e.Done()
	for {
		select {
		case <-e.ctx.Done():
			logger.Info("election loop context done, exiting")
			return
		default:
			logger.Info("starting election campaign")
			// Try to become the active node first
			err := e.Campaign(e.ctx)
			if err != nil {
				logger.Error("failed to campaign for election", zap.Error(err))
				continue
			}
			logger.Info("successfully became active node")
			e.role.Store(int32(RoleActive))
			if err := e.fn(); err != nil {
				logger.Error("failed to run function, becoming standby", zap.Error(err))
				e.role.Store(int32(RoleStandby))
				continue
			}
		}
	}
}

func (e *etcdElection) IsActive() bool {
	return Role(e.role.Load()) == RoleActive
}
