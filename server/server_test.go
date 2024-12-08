package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rleungx/tso/config"
	"github.com/rleungx/tso/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// Add helper function to start embedded etcd
func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, error) {
	tmpDir, err := os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := embed.NewConfig()
	cfg.Dir = tmpDir

	// Configure client URL
	lcurl, _ := url.Parse("http://127.0.0.1:0")
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}

	// Configure peer URL
	pcurl, _ := url.Parse("http://127.0.0.1:0")
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
		Host:           "127.0.0.1",
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
		Host:           "127.0.0.1",
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

func TestHTTPHandler(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = io.Discard

	// Start embedded etcd
	e, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer e.Close()
	time.Sleep(100 * time.Millisecond)

	clientURL := fmt.Sprintf("http://%s", e.Clients[0].Addr().String())
	cfg := &config.Config{
		Host:           "127.0.0.1",
		Port:           10001,
		Backend:        "etcd",
		BackendAddress: clientURL,
	}

	s := NewServer(cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()
	defer s.Stop()

	client := &http.Client{
		Timeout: time.Second,
	}

	// Ensure server is ready to accept requests
	time.Sleep(100 * time.Millisecond)

	tests := []struct {
		name           string
		method         string
		path           string
		query          string
		body           io.Reader
		expectedStatus int
	}{
		{
			name:           "Get timestamp - no parameters",
			method:         "GET",
			path:           "/timestamp",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Get timestamp - with count parameter",
			method:         "GET",
			path:           "/timestamp",
			query:          "count=5",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Unsupported method",
			method:         "POST",
			path:           "/timestamp",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Invalid path",
			method:         "GET",
			path:           "/invalid",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urlStr := fmt.Sprintf("http://%s:%d%s", cfg.Host, 10001, tt.path)
			if tt.query != "" {
				urlStr += "?" + tt.query
			}
			req, err := http.NewRequest(tt.method, urlStr, tt.body)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tt.expectedStatus, resp.StatusCode)

			if resp.StatusCode == http.StatusOK {
				var result struct {
					Timestamp struct {
						Physical int64 `json:"physical"`
						Logical  int64 `json:"logical"`
					} `json:"timestamp"`
					Count uint32 `json:"count"`
				}
				err = json.NewDecoder(resp.Body).Decode(&result)
				require.NoError(t, err)
				require.NotZero(t, result.Timestamp.Physical)
			}
		})
	}
}

func TestHTTPHandlerConcurrency(t *testing.T) {
	// Start embedded etcd
	e, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer e.Close()
	time.Sleep(100 * time.Millisecond)

	clientURL := fmt.Sprintf("http://%s", e.Clients[0].Addr().String())
	cfg := &config.Config{
		Host:           "127.0.0.1",
		Port:           10002,
		Backend:        "etcd",
		BackendAddress: clientURL,
	}

	s := NewServer(cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start()
	}()
	defer s.Stop()

	time.Sleep(100 * time.Millisecond)

	// Concurrency test
	concurrency := 10
	requests := 100
	var wg sync.WaitGroup
	type Timestamp struct {
		Physical int64
		Logical  int64
	}
	timestamps := make([]Timestamp, concurrency*requests)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := &http.Client{}

			for j := 0; j < requests; j++ {
				url := fmt.Sprintf("http://%s:%d/timestamp", cfg.Host, 10002)
				resp, err := client.Get(url)
				require.NoError(t, err)

				var result struct {
					Timestamp struct {
						Physical int64 `json:"physical"`
						Logical  int64 `json:"logical"`
					} `json:"timestamp"`
					Count uint32 `json:"count"`
				}
				err = json.NewDecoder(resp.Body).Decode(&result)
				require.NoError(t, err)
				resp.Body.Close()

				idx := workerID*requests + j
				timestamps[idx].Physical = result.Timestamp.Physical
				timestamps[idx].Logical = result.Timestamp.Logical
			}
		}(i)
	}

	wg.Wait()

	// Verify timestamps are strictly increasing
	sort.Slice(timestamps, func(i, j int) bool {
		if timestamps[i].Physical != timestamps[j].Physical {
			return timestamps[i].Physical < timestamps[j].Physical
		}
		return timestamps[i].Logical < timestamps[j].Logical
	})

	for i := 1; i < len(timestamps); i++ {
		require.True(t,
			timestamps[i].Physical > timestamps[i-1].Physical ||
				(timestamps[i].Physical == timestamps[i-1].Physical && timestamps[i].Logical > timestamps[i-1].Logical),
			"Timestamps must be strictly increasing")
	}
}
