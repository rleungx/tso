package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rleungx/tso/config"
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
	ctx    context.Context
	cancel context.CancelFunc

	proto.UnimplementedTSOServer

	config          *config.Config
	storage         storage.Storage
	timestampOracle *tso.TimestampOracle
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

	s.timestampOracle = tso.NewTimestampOracle(s.ctx, s.storage)
	s.timestampOracle.SyncTimestamp(s.storage)
	go s.timestampOracle.UpdateTimestamp(s.storage)

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

		physical, logical := s.timestampOracle.GenerateTimestamp(reqStream.Context(), req.GetCount())

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
