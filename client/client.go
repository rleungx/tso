package client

import (
	"context"
	"errors"
	"sync"

	"github.com/rleungx/tso/proto"
)

const (
	defaultRequestQueueSize = 100000
	defaultMaxBatchSize     = 100
	defaultMinBatchSize     = 1
	defaultMaxWaitTime      = 0
)

var reqPool = sync.Pool{
	New: func() interface{} {
		return &timestampRequest{
			done:    make(chan *proto.Timestamp, 1),
			errChan: make(chan error, 1),
		}
	},
}

type TSOFuture interface {
	// Wait gets the physical and logical time, it would block caller if data is not available yet.
	Wait() (int64, int64, error)
}

// Client state
type TSOClient struct {
	options  *options
	conn     *connManager
	requests chan *timestampRequest
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// Single timestamp request
type timestampRequest struct {
	ctx     context.Context
	done    chan *proto.Timestamp
	errChan chan error
}

// Wait implements the TSOFuture interface
func (r *timestampRequest) Wait() (int64, int64, error) {
	select {
	case ts := <-r.done:
		select {
		case <-r.ctx.Done():
			return 0, 0, r.ctx.Err()
		default:
			physical, logical := ts.Physical, ts.Logical
			reqPool.Put(r)
			return physical, logical, nil
		}
	case err := <-r.errChan:
		reqPool.Put(r)
		return 0, 0, err
	}
}

// NewTSOClient creates a new TSO client
func NewTSOClient(endpoints []string, opts ...Option) (*TSOClient, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("empty endpoints")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &TSOClient{
		requests: make(chan *timestampRequest, 10000),
		wg:       sync.WaitGroup{},
		ctx:      ctx,
		cancel:   cancel,
		options: &options{
			maxBatchSize: defaultMaxBatchSize,
			minBatchSize: defaultMinBatchSize,
			maxWaitTime:  defaultMaxWaitTime,
		},
		conn: &connManager{
			endpoints: endpoints,
		},
	}

	// Initialize the first connection
	if err := c.conn.switchToNextEndpoint(); err != nil {
		cancel()
		return nil, err
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	c.wg.Add(1)
	go c.batchProcessor()

	return c, nil
}

// GetTimestamp asynchronously gets a timestamp and returns a TSOFuture
func (c *TSOClient) GetTimestamp(ctx context.Context) (TSOFuture, error) {
	req := reqPool.Get().(*timestampRequest)
	req.ctx = ctx
	select {
	case <-req.done:
	default:
	}
	select {
	case <-req.errChan:
	default:
	}

	select {
	case c.requests <- req:
	case <-ctx.Done():
		reqPool.Put(req)
		return nil, ctx.Err()
	default:
		reqPool.Put(req)
		return nil, errors.New("request queue is full")
	}

	return req, nil
}

// Close closes the client
func (c *TSOClient) Close() {
	c.cancel()
	c.conn.connMu.Lock()
	if c.conn.conn != nil {
		c.conn.conn.Close()
	}
	c.conn.connMu.Unlock()
	c.wg.Wait()
}
