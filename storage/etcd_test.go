package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
	"google.golang.org/grpc/grpclog"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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

func TestEtcdClient(t *testing.T) {
	// Set up common test environment
	grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(io.Discard, io.Discard, io.Discard, 0))

	etcd, err := startEmbeddedEtcd(t)
	if err != nil {
		t.Fatal("Failed to start embedded etcd:", err)
	}
	defer etcd.Close()

	clientURL := etcd.Clients[0].Addr().String()
	endpoints := []string{"http://" + clientURL}
	timeout := 2 * time.Second

	// Create a reusable client creation function
	createClient := func() (Storage, error) {
		return NewEtcdClient(endpoints, timeout)
	}

	t.Run("SaveAndLoadTimestamp", func(t *testing.T) {
		client, err := createClient()
		require.NoError(t, err)
		defer client.Close()

		testCases := []struct {
			name    string
			time    time.Time
			wantErr bool
		}{
			{
				name:    "Normal time",
				time:    time.Now().UTC().Round(time.Second),
				wantErr: false,
			},
			{
				name:    "Zero time",
				time:    time.Time{},
				wantErr: false,
			},
			{
				name:    "Past time",
				time:    time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				wantErr: false,
			},
			{
				name:    "Future time",
				time:    time.Now().Add(24 * time.Hour).UTC().Round(time.Second),
				wantErr: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := client.SaveTimestamp(tc.time)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				loaded, err := client.LoadTimestamp()
				require.NoError(t, err)
				require.Equal(t, tc.time.UTC(), loaded.UTC())
			})
		}
	})

	t.Run("LoadNonExistentTimestamp", func(t *testing.T) {
		client, err := createClient()
		require.NoError(t, err)
		defer client.Close()

		// Clear existing timestamp
		etcdClient := client.(*EtcdClient)
		_, err = etcdClient.Client.Delete(context.Background(), "lastTimestamp")
		require.NoError(t, err)

		loaded, err := client.LoadTimestamp()
		require.NoError(t, err)
		require.True(t, loaded.IsZero())
	})

	t.Run("CloseClient", func(t *testing.T) {
		client, err := createClient()
		require.NoError(t, err)

		// Test normal close
		err = client.Close()
		require.NoError(t, err)

		// Test repeated close
		err = client.Close()
		require.NoError(t, err)

		// Test operations after close - should return an error but not panic
		_, err = client.LoadTimestamp()
		require.Error(t, err)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		client, err := createClient()
		require.NoError(t, err)
		defer client.Close()

		var wg sync.WaitGroup
		concurrency := 5
		wg.Add(concurrency)

		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				now := time.Now().UTC().Round(time.Second)
				require.NoError(t, client.SaveTimestamp(now))
				_, err := client.LoadTimestamp()
				require.NoError(t, err)
			}()
		}

		wg.Wait()
	})
}
