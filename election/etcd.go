package election

import (
	"context"
	"fmt"
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
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
	ctx      context.Context
	cancel   context.CancelFunc
}

func newEtcdElection(ctx context.Context, client *clientv3.Client) (Election, error) {
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
	}

	return e, nil
}

func (e *etcdElection) Watch(ctx context.Context) {
	watchChan := e.election.Observe(e.ctx)

	for {
		select {
		case resp, ok := <-watchChan:
			if !ok {
				logger.Info("Watch channel closed")
				return
			}
			if len(resp.Kvs) == 0 {
				logger.Info("No key-value pairs in response")
				return
			}
			logger.Info("Observed leader", zap.ByteString("leader", resp.Kvs[0].Value))
		case <-ctx.Done():
			logger.Info("Context done")
			return
		case <-e.ctx.Done():
			logger.Info("Election context done")
			return
		}
	}
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
