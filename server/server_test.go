package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rleungx/tso/config"
	"github.com/rleungx/tso/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Add helper function to start embedded etcd server
func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, error) {
	tmpDir, err := os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := embed.NewConfig()
	cfg.Dir = tmpDir

	// Configure client URL
	lcurl, _ := url.Parse("http://localhost:0")
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}

	// Configure peer URL
	pcurl, _ := url.Parse("http://localhost:0")
	cfg.ListenPeerUrls = []url.URL{*pcurl}
	cfg.AdvertisePeerUrls = []url.URL{*pcurl}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, cfg.AdvertisePeerUrls[0].String())
	cfg.ClusterState = embed.ClusterStateFlagNew

	cfg.LogLevel = "fatal"
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"/dev/null"}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(3 * time.Second):
		e.Close()
		return nil, fmt.Errorf("etcd took too long to start")
	}

	return e, nil
}

func TestServerStartStop(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard
	// Start embedded etcd
	e, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer e.Close()
	// Wait for etcd to fully start
	time.Sleep(100 * time.Millisecond)

	// Get the actual etcd client address
	clientURL := fmt.Sprintf("http://%s", e.Clients[0].Addr().String())
	// Create test configuration
	cfg := &config.Config{
		Host:           "localhost",
		Port:           0,
		Backend:        "etcd", // Use etcd storage for testing
		BackendAddress: clientURL,
	}

	// Create server instance
	s := NewServer(cfg)

	// Use channel to synchronize start
	errCh := make(chan error)
	go func() {
		errCh <- s.Start()
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Stop server
	err = s.Stop()
	require.NoError(t, err)

	// Check if there was an error during start
	startErr := <-errCh
	// Start returning an error is normal because we actively stopped the server
	require.Error(t, startErr)
}

func TestGracefulShutdown(t *testing.T) {
	// Start embedded etcd
	e, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer e.Close()
	// Wait for etcd to fully start
	time.Sleep(100 * time.Millisecond)

	// Get the actual etcd client address
	clientURL := fmt.Sprintf("http://%s", e.Clients[0].Addr().String())

	cfg := &config.Config{
		Host:           "localhost",
		Port:           10000,
		Backend:        "etcd",
		BackendAddress: clientURL,
	}

	s := NewServer(cfg)

	// Start server
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()

	// Wait for server to fully start
	time.Sleep(2 * time.Second)

	// Verify server started successfully
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// nolint:staticcheck
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err, "Failed to connect to gRPC server")
	defer conn.Close()

	var wg sync.WaitGroup

	// gRPC client test
	wg.Add(1)
	go func() {
		defer wg.Done()
		client := proto.NewTSOClient(conn)
		stream, err := client.GetTimestamp(context.Background())
		require.NoError(t, err)

		// Send some streaming requests
		for i := 0; i < 100; i++ {
			err = stream.Send(&proto.GetTimestampRequest{Count: 10})
			if err != nil {
				return
			}
			_, err = stream.Recv()
			if err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// HTTP client test
	wg.Add(1)
	go func() {
		defer wg.Done()
		httpClient := &http.Client{}

		// Send some HTTP requests
		for i := 0; i < 100; i++ {
			resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/timestamp", cfg.Host, cfg.Port))
			if err != nil {
				return
			}
			resp.Body.Close()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Wait for requests to start processing
	time.Sleep(200 * time.Millisecond)

	// Stop server
	err = s.Stop()
	require.NoError(t, err)

	// Wait for all requests to complete
	wg.Wait()

	// Verify gRPC server is shut down
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// nolint:staticcheck
	_, err = grpc.DialContext(ctx, fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.Error(t, err, "gRPC server should be completely shutdown")

	// Verify HTTP server is shut down
	_, err = http.Get(fmt.Sprintf("http://%s:%d/timestamp", cfg.Host, cfg.Port))
	require.Error(t, err, "HTTP server should be completely shutdown")
}
