package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rleungx/tso/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type TSOClient struct {
	client    proto.TSOClient
	requests  chan *timestampRequest
	batchSize uint32
	maxWait   time.Duration
	grpcOpts  []grpc.DialOption
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	endpoints []string         // All available server addresses
	current   int              // Current server index
	connMu    sync.RWMutex     // Protects connection-related fields
	conn      *grpc.ClientConn // Current connection
	tlsConfig *tls.Config      // TLS configuration
}

// Single timestamp request
type timestampRequest struct {
	done    chan *proto.Timestamp
	errChan chan error
}

// NewTSOClient creates a new TSO client
func NewTSOClient(endpoints []string, opts ...Option) (*TSOClient, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("empty endpoints")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &TSOClient{
		endpoints: endpoints,
		requests:  make(chan *timestampRequest, 10000),
		batchSize: 100,
		maxWait:   time.Millisecond * 10,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Initialize the first connection
	if err := c.switchToNextEndpoint(); err != nil {
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

// WithGRPCDialOption sets the grpc dial options
func WithGRPCDialOption(opts ...grpc.DialOption) Option {
	return func(c *TSOClient) {
		c.grpcOpts = opts
	}
}

// WithTLSConfig sets the TLS configuration
func WithTLSConfig(config *tls.Config) Option {
	return func(c *TSOClient) {
		c.tlsConfig = config
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

	maxRetries := len(c.endpoints) * 2 // Maximum retries is twice the number of endpoints
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			// Switch to the next endpoint before retrying
			if err := c.switchToNextEndpoint(); err != nil {
				lastErr = err
				continue
			}

			// Retry wait
			select {
			case <-time.After(time.Second):
			case <-c.ctx.Done():
				c.failAllRequests(requests, c.ctx.Err())
				return
			}
		}

		// Get the current client
		c.connMu.RLock()
		client := c.client
		c.connMu.RUnlock()

		stream, err := client.GetTimestamp(c.ctx)
		if err != nil {
			lastErr = err
			if shouldRetry(err) {
				continue
			}
			c.failAllRequests(requests, err)
			return
		}
		defer stream.CloseSend()

		// Send batch request
		if err := stream.Send(&proto.GetTimestampRequest{
			Count: uint32(len(requests)),
		}); err != nil {
			lastErr = err
			if shouldRetry(err) {
				continue
			}
			c.failAllRequests(requests, err)
			return
		}

		// Receive response
		resp, err := stream.Recv()
		if err != nil {
			lastErr = err
			if shouldRetry(err) {
				continue
			}
			c.failAllRequests(requests, err)
			return
		}

		// Successfully received response, distribute results
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
		return
	}

	c.failAllRequests(requests, fmt.Errorf("max retries reached, last error: %v", lastErr))
}

// failAllRequests handles batch errors
func (c *TSOClient) failAllRequests(requests []*timestampRequest, err error) {
	for _, req := range requests {
		req.errChan <- err
		close(req.done)
		close(req.errChan)
	}
}

// switchToNextEndpoint switches to the next available endpoint
func (c *TSOClient) switchToNextEndpoint() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	// Close existing connection
	if c.conn != nil {
		c.conn.Close()
	}

	// Try all endpoints
	startIndex := c.current
	for i := 0; i < len(c.endpoints); i++ {
		c.current = (startIndex + i) % len(c.endpoints)
		endpoint := c.endpoints[c.current]

		opts := []grpc.DialOption{}

		// Choose certificate based on whether TLS is configured
		if c.tlsConfig != nil {
			opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig)))
		} else {
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		// nolint:staticcheck
		opts = append(opts, grpc.WithBlock())
		opts = append(opts, c.grpcOpts...)

		// Set connection timeout
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		// nolint:staticcheck
		conn, err := grpc.DialContext(ctx, endpoint, opts...)
		cancel()

		if err == nil {
			c.conn = conn
			c.client = proto.NewTSOClient(conn)
			return nil
		}
	}

	return errors.New("failed to connect to any endpoint")
}

// Close closes the client
func (c *TSOClient) Close() {
	c.cancel()
	c.connMu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.connMu.Unlock()
	c.wg.Wait()
}

// shouldRetry determines whether the request should be retried
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Check if it is a gRPC error
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	// Determine whether to retry based on the error code
	switch st.Code() {
	case codes.Unavailable, // Service unavailable
		codes.DeadlineExceeded, // Request timeout
		codes.Aborted,          // Operation aborted
		codes.Internal,         // Internal error
		codes.Unknown:          // Unknown error
		return true
	}

	// Check if the error message contains specific keywords
	errMsg := st.Message()
	switch {
	case strings.Contains(errMsg, "connection refused"),
		strings.Contains(errMsg, "no connection"),
		strings.Contains(errMsg, "transport failure"),
		strings.Contains(errMsg, "logical clock overflow"),
		strings.Contains(errMsg, "connection closed"):
		return true
	}

	return false
}
