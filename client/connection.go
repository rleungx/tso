package client

import (
	"context"
	"crypto/tls"
	"errors"
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

// Connection management
type connManager struct {
	endpoints []string         // All available server addresses
	current   int              // Current server index
	connMu    sync.RWMutex     // Protects connection-related fields
	conn      *grpc.ClientConn // Current connection
	client    proto.TSOClient
	grpcOpts  []grpc.DialOption // gRPC connection options
	tlsConfig *tls.Config       // TLS configuration
}

// switchToNextEndpoint switches to the next available endpoint
func (c *connManager) switchToNextEndpoint() error {
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
