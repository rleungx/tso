package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
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

// tsoUpdatePhysicalInterval is the interval to update the physical part of a tso.
var tsoUpdatePhysicalInterval = 50 * time.Millisecond

// ServiceRole represents the service role
type ServiceRole int32

const (
	// RoleStandby indicates standby status, not providing service
	RoleStandby ServiceRole = iota
	// RoleActive indicates active status, providing service
	RoleActive
)

func (r ServiceRole) String() string {
	switch r {
	case RoleStandby:
		return "Standby"
	case RoleActive:
		return "Active"
	default:
		return "Unknown"
	}
}

func (s *Server) IsActive() bool {
	return ServiceRole(s.role.Load()) == RoleActive
}

type Server struct {
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	proto.UnimplementedTSOServer

	config          *config.Config
	storage         storage.Storage
	timestampOracle *tso.TimestampOracle
	election        election.Election
	role            atomic.Int32
}

// NewServer creates a new server instance
func NewServer(config *config.Config) *Server {
	return &Server{
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
	case "mem":
		s.storage, err = storage.NewMemStorage()
	}
	if err != nil {
		return err
	}

	// Initialize election
	s.election, err = election.NewElection(s.ctx, s.storage)
	if err != nil {
		return err
	}

	go s.electionLoop()

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
	var grpcServer *grpc.Server
	if tlsEnabled {
		cert, err := loadTLSCredentials(s.config.CertFile, s.config.KeyFile)
		if err != nil {
			return err
		}
		grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert}, // Use your certificate
		})))
	} else {
		grpcServer = grpc.NewServer() // Do not use TLS
	}
	// Register your gRPC service here
	proto.RegisterTSOServer(grpcServer, s)
	// Create matchers for gRPC and HTTP
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// Start gRPC server
	go func() {
		if err := grpcServer.Serve(grpcL); err != nil {
			logger.Error("gRPC server failed", zap.Error(err))
		}
	}()

	// Start HTTP server
	var httpServer *http.Server
	if tlsEnabled { // Check if TLS is enabled
		cert, err := loadTLSCredentials(s.config.CertFile, s.config.KeyFile)
		if err != nil {
			return err
		}
		httpServer = &http.Server{
			Addr: fmt.Sprintf("%s:%d", s.config.Host, s.config.Port), // Use the same port
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert}, // Use your certificate
			},
			Handler: s.setupRoutes(), // Set routes
		}
		go func() {
			if err := httpServer.ServeTLS(httpL, s.config.CertFile, s.config.KeyFile); err != nil {
				logger.Error("HTTP server failed", zap.Error(err))
			}
		}()
	} else {
		httpServer = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", s.config.Host, s.config.Port), // Use the same port
			Handler: s.setupRoutes(),                                    // Set routes
		}
		go func() {
			if err := httpServer.Serve(httpL); err != nil {
				logger.Error("HTTP server failed", zap.Error(err))
			}
		}()
	}

	// Start cmux
	return m.Serve()
}

func (s *Server) Stop() error {
	if err := s.storage.Close(); err != nil {
		return err
	}
	s.cancel()
	return nil
}

// setupRoutes sets up HTTP routes
func (s *Server) setupRoutes() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/timestamp", s.GetTS) // Register handler
	return router
}

// loadTLSCredentials loads TLS certificates
func loadTLSCredentials(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

// GetTimestamp implements the GetTimestamp method of the TSO service
func (s *Server) GetTimestamp(reqStream proto.TSO_GetTimestampServer) error {
	// Handle request stream and return timestamp
	for {
		req, err := reqStream.Recv()
		if err == io.EOF {
			break // Stream ended
		}
		if err != nil {
			return err
		}

		physical, logical, err := s.timestampOracle.GenerateTimestamp(reqStream.Context(), req.GetCount())
		if err != nil {
			return err
		}

		// Generate timestamp logic
		timestamp := &proto.Timestamp{
			Physical: physical,
			Logical:  logical,
		}

		// Send response
		if err := reqStream.Send(&proto.GetTimestampResponse{
			Count:     req.Count,
			Timestamp: timestamp,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) electionLoop() {
	for {
		select {
		case <-s.ctx.Done():
			logger.Info("election loop context done, exiting")
			return
		default:
			logger.Info("starting election campaign")
			// Try to become the active node first
			err := s.election.Campaign(s.ctx)
			if err != nil {
				logger.Error("failed to campaign for election", zap.Error(err))
				s.becomeStandby()
				logger.Info("watching for election changes")
				s.election.Watch(s.ctx)
				continue
			}

			s.becomeActive()
			// If handleActiveState returns, it means re-election is needed
			logger.Info("exited active state, will retry election")
		}
	}
}

func (s *Server) becomeActive() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ServiceRole(s.role.Load()) == RoleActive {
		return nil
	}

	logger.Info("becoming active node")

	// 1. Initialize TimestampOracle
	if s.timestampOracle == nil {
		s.timestampOracle = tso.NewTimestampOracle(s.ctx, s.storage)
	}

	// 2. Sync timestamp
	if err := s.timestampOracle.SyncTimestamp(s.storage); err != nil {
		logger.Error("failed to sync timestamp", zap.Error(err))
		if resignErr := s.election.Resign(); resignErr != nil {
			logger.Error("failed to resign from election", zap.Error(resignErr))
		}
		return err
	}
	defer s.timestampOracle.Reset()
	// 4. Update role
	s.role.Store(int32(RoleActive))
	logger.Info("became active node")
	defer s.role.Store(int32(RoleStandby))
	ticker := time.NewTicker(tsoUpdatePhysicalInterval)
	defer ticker.Stop()

	// 3. Start timestamp update
	for {
		select {
		case <-s.ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.timestampOracle.UpdateTimestamp(s.storage); err != nil {
				logger.Error("failed to update timestamp", zap.Error(err))
				if resignErr := s.election.Resign(); resignErr != nil {
					logger.Error("failed to resign from election after update failure", zap.Error(resignErr))
				}
			}
		}
	}
}

func (s *Server) becomeStandby() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ServiceRole(s.role.Load()) == RoleStandby {
		return nil
	}

	// 2. Update role
	s.role.Store(int32(RoleStandby))
	logger.Info("became standby node")
	return nil
}
