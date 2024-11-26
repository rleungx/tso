package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
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

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-e.Server.ReadyNotify():
		t.Log("Embedded etcd is ready")
	case <-time.After(10 * time.Second):
		e.Close()
		return nil, fmt.Errorf("etcd took too long to start")
	}

	return e, nil
}

func TestEtcdClient(t *testing.T) {
	etcd, err := startEmbeddedEtcd(t)
	if err != nil {
		t.Fatal("Failed to start embedded etcd:", err)
	}
	defer etcd.Close()

	// Use the actual listening client URL
	clientURL := etcd.Clients[0].Addr().String()
	endpoints := []string{"http://" + clientURL}
	t.Logf("Using etcd endpoint: %s", endpoints[0])
	timeout := 5 * time.Second

	t.Run("NewEtcdClient", func(t *testing.T) {
		// Test normal creation
		client, err := NewEtcdClient(endpoints, timeout)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		defer client.Close()

		// Test empty endpoints - should return an error
		_, err = NewEtcdClient(nil, timeout)
		assert.Error(t, err)

		// Test zero timeout - should return an error
		_, err = NewEtcdClient(endpoints, 0)
		assert.Error(t, err)
	})

	t.Run("SaveAndLoadTimestamp", func(t *testing.T) {
		client, err := NewEtcdClient(endpoints, timeout)
		assert.NoError(t, err)
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
					assert.Error(t, err)
					return
				}
				assert.NoError(t, err)

				loaded, err := client.LoadTimestamp()
				assert.NoError(t, err)
				assert.Equal(t, tc.time.UTC(), loaded.UTC())
			})
		}
	})

	t.Run("LoadNonExistentTimestamp", func(t *testing.T) {
		client, err := NewEtcdClient(endpoints, timeout)
		assert.NoError(t, err)
		defer client.Close()

		// Clear existing timestamp
		etcdClient := client.(*EtcdClient)
		_, err = etcdClient.Client.Delete(context.Background(), "lastTimestamp")
		assert.NoError(t, err)

		loaded, err := client.LoadTimestamp()
		assert.NoError(t, err)
		assert.True(t, loaded.IsZero())
	})

	t.Run("ConnectionError", func(t *testing.T) {
		badEndpoints := []string{"localhost:1"}
		client, err := NewEtcdClient(badEndpoints, time.Second)
		if err != nil {
			return
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		_, err = client.(*EtcdClient).Client.Get(ctx, "test-key")
		assert.Error(t, err)
	})

	t.Run("CloseClient", func(t *testing.T) {
		client, err := NewEtcdClient(endpoints, timeout)
		assert.NoError(t, err)

		// Test normal close
		err = client.Close()
		assert.NoError(t, err)

		// Test repeated close
		err = client.Close()
		assert.NoError(t, err)

		// Test operations after close - should return an error but not panic
		_, err = client.LoadTimestamp()
		assert.Error(t, err)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		client, err := NewEtcdClient(endpoints, timeout)
		assert.NoError(t, err)
		defer client.Close()

		concurrency := 10
		done := make(chan bool)

		for i := 0; i < concurrency; i++ {
			go func() {
				now := time.Now().UTC().Round(time.Second)
				err := client.SaveTimestamp(now)
				assert.NoError(t, err)

				_, err = client.LoadTimestamp()
				assert.NoError(t, err)
				done <- true
			}()
		}

		for i := 0; i < concurrency; i++ {
			<-done
		}
	})
}
