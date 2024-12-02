package election

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// Add helper function to start embedded etcd server
func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, error) {
	tmpDir, err := os.MkdirTemp("", "tso-test-*")
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
		time.Sleep(100 * time.Millisecond)
	case <-time.After(5 * time.Second):
		e.Close()
		return nil, fmt.Errorf("etcd took too long to start")
	}

	return e, nil
}

func newTestClient(t *testing.T, clientURL string) *clientv3.Client {
	u, err := url.Parse(clientURL)
	require.NoError(t, err)

	// Create a zap logger that disables all output
	lg := zap.NewNop()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            []string{u.Host},
		DialTimeout:          2 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 500 * time.Millisecond,
		AutoSyncInterval:     0,
		Logger:               lg, // Use nop logger
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = cli.Status(ctx, clientURL)
	require.NoError(t, err)

	_, err = cli.Delete(ctx, defaultElectionPrefix, clientv3.WithPrefix())
	require.NoError(t, err)

	return cli
}

func TestCampaign(t *testing.T) {
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	clientURL := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())
	cli := newTestClient(t, clientURL)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := cli.Delete(ctx, defaultElectionPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		cli.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create the first node, it will automatically become the leader
	leaderSelected := make(chan struct{})
	var leaderOnce sync.Once
	e1, err := newEtcdElection(ctx, cli, func() error {
		leaderOnce.Do(func() {
			close(leaderSelected)
		})
		return nil
	})
	require.NoError(t, err)
	defer e1.Close()

	// Wait for the first node to become the leader
	select {
	case <-leaderSelected:
		// The first node successfully became the leader
	case <-time.After(time.Second):
		t.Fatal("first node failed to become leader in time")
	}

	// Create the second node, it should not become the leader
	secondNodeTried := make(chan struct{})
	var secondOnce sync.Once
	e2, err := newEtcdElection(ctx, cli, func() error {
		secondOnce.Do(func() {
			close(secondNodeTried)
		})
		return nil
	})
	require.NoError(t, err)
	defer e2.Close()

	// Verify that the second node did not become the leader in a short time
	select {
	case <-secondNodeTried:
		t.Fatal("second node shouldn't become leader while first node is active")
	case <-time.After(500 * time.Millisecond):
		// As expected, the second node did not become the leader
	}
}

func TestResign(t *testing.T) {
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	clientURL := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	cli := newTestClient(t, clientURL)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create the first node and wait for it to become the leader
	leaderSelected := make(chan struct{})
	var leaderOnce sync.Once
	e1, err := newEtcdElection(ctx, cli, func() error {
		leaderOnce.Do(func() {
			close(leaderSelected)
		})
		return nil
	})
	require.NoError(t, err)
	defer e1.Close()

	// Wait for the first node to become the leader
	select {
	case <-leaderSelected:
		// Successfully became the leader
	case <-time.After(time.Second):
		t.Fatal("first node failed to become leader in time")
	}

	// Create the second node
	secondNodeSelected := make(chan struct{})
	var secondOnce sync.Once
	e2, err := newEtcdElection(ctx, cli, func() error {
		secondOnce.Do(func() {
			close(secondNodeSelected)
		})
		return nil
	})
	require.NoError(t, err)
	defer e2.Close()

	// Keep trying to resign until e2 successfully becomes the leader
	for i := 0; i < 10; i++ {
		err = e1.Resign()
		require.NoError(t, err)

		// Check if e2 became the leader
		select {
		case <-secondNodeSelected:
			return // Test succeeded
		case <-time.After(200 * time.Millisecond):
			continue // Keep trying
		}
	}
	t.Fatal("second node failed to become leader after multiple resign attempts")
}

func TestClose(t *testing.T) {
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	clientURL := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())
	cli := newTestClient(t, clientURL)
	defer cli.Close()

	ctx := context.Background()

	// Create a node and wait for it to become the leader
	leaderSelected := make(chan struct{})
	var leaderOnce sync.Once
	election, err := newEtcdElection(ctx, cli, func() error {
		leaderOnce.Do(func() {
			close(leaderSelected)
		})
		return nil
	})
	require.NoError(t, err)

	// Wait for the node to become the leader
	select {
	case <-leaderSelected:
		// Successfully became the leader
	case <-time.After(time.Second):
		t.Fatal("node failed to become leader in time")
	}

	// Test close
	err = election.Close()
	require.NoError(t, err)

	// Verify that it cannot participate in the election after closing
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = election.Campaign(ctxTimeout)
	require.Error(t, err)
}
