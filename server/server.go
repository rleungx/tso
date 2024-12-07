package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rleungx/tso/config"
	"github.com/rleungx/tso/election"
	"github.com/rleungx/tso/logger"
	"github.com/rleungx/tso/proto"
	"github.com/rleungx/tso/storage"
	"github.com/rleungx/tso/tso"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Server struct {
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	proto.UnimplementedTSOServer

	id              string
	config          *config.Config
	storage         storage.Storage
	timestampOracle *tso.TimestampOracle
	election        election.Election
	grpcServer      *grpc.Server
	httpServer      *http.Server
}

// NewServer creates a new server instance
func NewServer(config *config.Config) *Server {
	return &Server{
		id:     generateID(),
		config: config,
	}
}

// Start starts the server
func (s *Server) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	var err error
	switch s.config.Backend {
	case "etcd":
		s.storage, err = storage.NewEtcdClient([]string{s.config.BackendAddress}, 5*time.Second)
	case "consul":
		s.storage, err = storage.NewConsulClient(s.config.BackendAddress)
	case "redis":
		s.storage, err = storage.NewRedisClient(s.config.BackendAddress)
	case "mem":
		s.storage, err = storage.NewMemStorage()
	}
	if err != nil {
		return err
	}

	s.timestampOracle = tso.NewTimestampOracle(s.ctx, s.storage)

	// Initialize election
	s.election, err = election.NewElection(s.ctx, s.storage, s.id, s.timestampOracle.UpdateTimestampLoop)
	if err != nil {
		return err
	}

	// Create a listener
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Host, s.config.Port))
	if err != nil {
		return err
	}

	// Create cmux instance
	m := cmux.New(lis)

	// Check if certificate files exist
	tlsEnabled := s.config.CertFile != "" && s.config.KeyFile != ""

	// Create gRPC server
	if tlsEnabled {
		cert, err := loadTLSCredentials(s.config.CertFile, s.config.KeyFile)
		if err != nil {
			return err
		}
		s.grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert}, // Use your certificate
		})))
	} else {
		s.grpcServer = grpc.NewServer() // Do not use TLS
	}
	// Register your gRPC service here
	proto.RegisterTSOServer(s.grpcServer, s)
	// Create matchers for gRPC and HTTP
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// Start gRPC server
	go func() {
		if err := s.grpcServer.Serve(grpcL); err != nil {
			logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	// Start HTTP server
	if tlsEnabled { // Check if TLS is enabled
		cert, err := loadTLSCredentials(s.config.CertFile, s.config.KeyFile)
		if err != nil {
			return err
		}
		s.httpServer = &http.Server{
			Addr: fmt.Sprintf("%s:%d", s.config.Host, s.config.Port), // Use the same port
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert}, // Use your certificate
			},
			Handler: s.setupRoutes(), // Set routes
		}
		go func() {
			if err := s.httpServer.ServeTLS(httpL, s.config.CertFile, s.config.KeyFile); err != nil {
				logger.Error("HTTP server failed", zap.Error(err))
			}
		}()
	} else {
		s.httpServer = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", s.config.Host, s.config.Port), // Use the same port
			Handler: s.setupRoutes(),                                    // Set routes
		}
		go func() {
			if err := s.httpServer.Serve(httpL); err != nil {
				logger.Error("HTTP server failed", zap.Error(err))
			}
		}()
	}

	// Start cmux
	return m.Serve()
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Cancel context, notify all services to start the shutdown process
	// This will trigger all goroutines listening to the context to start exiting
	if s.cancel != nil {
		s.cancel()
	}

	// 2. Stop external services, no longer accept new requests
	// Need to wait for existing requests to be processed before continuing the shutdown process
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return err
		}
	}

	// 3. Stop the election service
	// This will cause the current node to exit the leader state, and the TSO service will also stop serving due to losing the leader
	if s.election != nil {
		s.election.Close()
	}

	// 4. Finally close the etcd client
	// At this point, all services that depend on etcd have stopped, and the client connection can be safely closed
	if s.storage != nil {
		s.storage.Close()
	}

	return nil
}

// loadTLSCredentials loads TLS certificates
func loadTLSCredentials(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

func generateID() string {
	// Timestamp occupies 41 bits, shift left by 22 bits
	timestampShift := 22
	// Get the current timestamp (milliseconds)
	timestamp := uint64(time.Now().UnixMilli())

	// Generate random number (22 bits)
	var randomBytes [8]byte
	_, err := rand.Read(randomBytes[:])
	if err != nil {
		random := uint64(time.Now().UnixNano() & ((1 << timestampShift) - 1))
		return fmt.Sprintf("%d", (timestamp<<timestampShift)|random)
	}

	// Limit the random number to 22 bits
	random := binary.BigEndian.Uint64(randomBytes[:]) & ((1 << timestampShift) - 1)

	// Combine timestamp and random number
	return fmt.Sprintf("%d", (timestamp<<timestampShift)|random)
}
