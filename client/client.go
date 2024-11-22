package client

import (
	"context"
	"sync"
	"time"

	"github.com/rleungx/tso/proto"

	"google.golang.org/grpc"
)

type TSOClient struct {
	client    proto.TSOClient
	requests  chan *timestampRequest
	batchSize uint32
	maxWait   time.Duration
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// Single timestamp request
type timestampRequest struct {
	done    chan *proto.Timestamp
	errChan chan error
}

// NewTSOClient creates a new TSO client
func NewTSOClient(conn *grpc.ClientConn, opts ...Option) *TSOClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &TSOClient{
		client:    proto.NewTSOClient(conn),
		requests:  make(chan *timestampRequest, 10000),
		batchSize: 100,                   // Default batch size
		maxWait:   time.Millisecond * 10, // Default max wait time
		ctx:       ctx,
		cancel:    cancel,
	}

	// Apply options
	for _, opt := range opts {
		opt(c)
	}

	// Start background batch processing
	c.wg.Add(1)
	go c.batchProcessor()

	return c
}

// Option defines client options
type Option func(*TSOClient)

// WithBatchSize sets the batch size
func WithBatchSize(size uint32) Option {
	return func(c *TSOClient) {
		c.batchSize = size
	}
}

// WithMaxWait sets the max wait time
func WithMaxWait(d time.Duration) Option {
	return func(c *TSOClient) {
		c.maxWait = d
	}
}

// GetTimestamp gets a single timestamp
func (c *TSOClient) GetTimestamp(ctx context.Context) (*proto.Timestamp, error) {
	req := &timestampRequest{
		done:    make(chan *proto.Timestamp, 1),
		errChan: make(chan error, 1),
	}

	// Send request to request pool
	select {
	case c.requests <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for result
	select {
	case ts := <-req.done:
		return ts, nil
	case err := <-req.errChan:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// batchProcessor is the background batch processor
func (c *TSOClient) batchProcessor() {
	defer c.wg.Done()

	var (
		pending []*timestampRequest
		timer   = time.NewTimer(c.maxWait)
	)
	defer timer.Stop()

	for {
		timer.Reset(c.maxWait)
		select {
		case <-c.ctx.Done():
			// Process remaining requests
			c.processBatch(pending)
			return

		case req := <-c.requests:
			pending = append(pending, req)

			// If batch size is reached, process immediately
			if uint32(len(pending)) >= c.batchSize {
				c.processBatch(pending)
				pending = pending[:0]
				timer.Stop()
			}

		case <-timer.C:
			// Timeout, process current batch
			if len(pending) > 0 {
				c.processBatch(pending)
				pending = pending[:0]
			}
		}
	}
}

// processBatch processes a batch of requests
func (c *TSOClient) processBatch(requests []*timestampRequest) {
	if len(requests) == 0 {
		return
	}

	stream, err := c.client.GetTimestamp(c.ctx)
	if err != nil {
		c.failAllRequests(requests, err)
		return
	}
	defer stream.CloseSend()

	// Send batch request
	if err := stream.Send(&proto.GetTimestampRequest{
		Count: uint32(len(requests)),
	}); err != nil {
		c.failAllRequests(requests, err)
		return
	}

	// Receive response
	resp, err := stream.Recv()
	if err != nil {
		c.failAllRequests(requests, err)
		return
	}

	// Distribute results
	baseTimestamp := resp.Timestamp
	for i, req := range requests {
		ts := &proto.Timestamp{
			Physical: baseTimestamp.Physical,
			Logical:  baseTimestamp.Logical + int64(i),
		}
		req.done <- ts
		close(req.done)
		close(req.errChan)
	}
}

// failAllRequests handles batch errors
func (c *TSOClient) failAllRequests(requests []*timestampRequest, err error) {
	for _, req := range requests {
		req.errChan <- err
		close(req.done)
		close(req.errChan)
	}
}

// Close closes the client
func (c *TSOClient) Close() {
	c.cancel()
	c.wg.Wait()
}
