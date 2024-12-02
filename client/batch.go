package client

import (
	"fmt"
	"time"

	"github.com/rleungx/tso/proto"
)

// batchProcessor is the background batch processor
func (c *TSOClient) batchProcessor() {
	defer c.wg.Done()
	var currentBatchSize uint32
	pending := make([]*timestampRequest, 0, c.options.maxBatchSize)
	timer := time.NewTimer(c.options.maxWaitTime)
	defer timer.Stop()

	for {
		timer.Reset(c.options.maxWaitTime)
		select {
		case <-c.ctx.Done():
			c.processBatch(pending)
			return

		case req := <-c.requests:
			pending = append(pending, req)

			queueLength := len(c.requests)
			if queueLength > int(c.options.maxBatchSize) {
				currentBatchSize = c.options.maxBatchSize
			} else if queueLength > int(c.options.minBatchSize) {
				currentBatchSize = uint32(queueLength)
			} else {
				currentBatchSize = c.options.minBatchSize
			}

			if uint32(len(pending)) >= currentBatchSize {
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

	maxRetries := len(c.conn.endpoints) * 2 // Maximum retries is twice the number of endpoints
	var (
		err, lastErr error
		stream       proto.TSO_GetTimestampClient
	)

	defer func() {
		if stream != nil {
			stream.CloseSend()
		}
	}()
	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			// Retry wait
			select {
			case <-time.After(time.Second):
			case <-c.ctx.Done():
				c.failAllRequests(requests, c.ctx.Err())
				return
			}
			// Switch to the next endpoint before retrying
			if err := c.conn.switchToNextEndpoint(); err != nil {
				lastErr = err
				continue
			}
		}

		// Recreate stream (only after switching endpoint)
		c.conn.connMu.RLock()
		client := c.conn.client
		c.conn.connMu.RUnlock()

		stream, err = client.GetTimestamp(c.ctx)
		if err != nil {
			lastErr = err
			if shouldRetry(err) {
				continue
			}
			c.failAllRequests(requests, err)
			return
		}

		// Send batch request
		err := stream.Send(&proto.GetTimestampRequest{
			Count: uint32(len(requests)),
		})
		if err != nil {
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
		for i, req := range requests {
			// Create new Timestamp, only copy specific values instead of the entire struct
			ts := &proto.Timestamp{
				Physical: resp.Timestamp.Physical,
				Logical:  resp.Timestamp.Logical + int64(i),
			}
			req.done <- ts
		}
		return
	}

	c.failAllRequests(requests, fmt.Errorf("max retries reached, last error: %v", lastErr))
}

// failAllRequests handles batch errors
func (c *TSOClient) failAllRequests(requests []*timestampRequest, err error) {
	for _, req := range requests {
		req.errChan <- err
	}
}
