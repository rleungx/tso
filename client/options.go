package client

import (
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
)

// Client options
type options struct {
	maxBatchSize uint32
	minBatchSize uint32
	maxWaitTime  time.Duration
}

type Option func(*TSOClient)

func WithMaxBatchSize(size uint32) Option {
	return func(c *TSOClient) {
		c.options.maxBatchSize = size
	}
}

func WithMinBatchSize(size uint32) Option {
	return func(c *TSOClient) {
		c.options.minBatchSize = size
	}
}

func WithMaxWaitTime(d time.Duration) Option {
	return func(c *TSOClient) {
		c.options.maxWaitTime = d
	}
}

func WithGRPCDialOption(opts ...grpc.DialOption) Option {
	return func(c *TSOClient) {
		c.conn.grpcOpts = opts
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(c *TSOClient) {
		c.conn.tlsConfig = config
	}
}
