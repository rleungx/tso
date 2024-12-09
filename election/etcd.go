package election

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rleungx/tso/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	defaultElectionPrefix = "/election"
)

type etcdElection struct {
	sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	id       string
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	fn       func() error
	isActive bool
}

func newEtcdElection(ctx context.Context, client *clientv3.Client, id string, fn func() error) (Election, error) {
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
		id:       id,
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

func (e *etcdElection) Campaign() error {
	// Check if election instance exists
	if e.election == nil {
		return fmt.Errorf("election instance is nil")
	}

	return e.election.Campaign(e.ctx, e.id)
}

func (e *etcdElection) electionLoop() {
	defer e.Done()
	for {
		select {
		case <-e.ctx.Done():
			logger.Info("election loop context done, exiting")
			return
		default:
			err := e.Campaign()
			if err != nil {
				logger.Error("failed to campaign for election", zap.Error(err))
				continue
			}
			logger.Info("successfully became active node")
			e.isActive = true
			if err := e.fn(); err != nil {
				logger.Error("failed to run function, step down", zap.Error(err))
				e.isActive = false
				continue
			}
		}
	}
}

func (e *etcdElection) IsActive() bool {
	return e.isActive
}
