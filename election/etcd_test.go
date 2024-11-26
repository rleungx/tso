package election

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func newEmbedEtcd(t *testing.T) (*embed.Etcd, string, int) {
	dir, err := os.MkdirTemp("", "etcd-test-*")
	require.NoError(t, err)

	// Configure embedded etcd
	cfg := embed.NewConfig()
	cfg.Dir = dir
	lcurl, _ := url.Parse("http://localhost:0")
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}
	lpurl, _ := url.Parse("http://localhost:0")
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.AdvertisePeerUrls = []url.URL{*lpurl}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, lpurl)

	// Start embedded etcd
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)

	select {
	case <-e.Server.ReadyNotify():
		// etcd server is ready
	case <-time.After(3 * time.Second):
		e.Close()
		t.Fatal("etcd server took too long to start")
	}

	clientURL := e.Clients[0].Addr().String()
	_, portStr, _ := net.SplitHostPort(clientURL)
	clientPort, _ := strconv.Atoi(portStr)

	return e, clientURL, clientPort
}

func newTestClient(t *testing.T, clientURL string) *clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://%s", clientURL)},
		DialTimeout: 5 * time.Second,
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
	etcd, clientURL, _ := newEmbedEtcd(t)
	defer func() {
		etcd.Close()
		os.RemoveAll(etcd.Config().Dir)
	}()

	cli := newTestClient(t, clientURL)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := cli.Delete(ctx, defaultElectionPrefix, clientv3.WithPrefix())
		require.NoError(t, err)
		cli.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	e1, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)
	defer func() {
		err := e1.Close()
		require.NoError(t, err)
	}()

	// Test the first node successfully campaigns
	err = e1.Campaign(ctx)
	require.NoError(t, err)

	// Test the second node cannot become the leader
	e2, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)
	defer func() {
		err := e2.Close()
		require.NoError(t, err)
	}()

	// Use a shorter timeout
	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	// Try to let the second node campaign
	err = e2.Campaign(ctxTimeout)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")
}

func TestWatch(t *testing.T) {
	etcd, clientURL, _ := newEmbedEtcd(t)
	defer func() {
		etcd.Close()
		os.RemoveAll(etcd.Config().Dir)
	}()

	cli := newTestClient(t, clientURL)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)
	defer e.Close()

	watchDone := make(chan struct{})
	go func() {
		e.Watch(ctx)
		close(watchDone)
	}()

	cancel()
	select {
	case <-watchDone:
		// As expected
	case <-time.After(2 * time.Second):
		t.Fatal("Watch should exit after context cancel")
	}
}

func TestResign(t *testing.T) {
	etcd, clientURL, _ := newEmbedEtcd(t)
	defer func() {
		etcd.Close()
		os.RemoveAll(etcd.Config().Dir)
	}()

	cli := newTestClient(t, clientURL)
	defer cli.Close()

	ctx := context.Background()
	election1, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)
	defer election1.Close()

	// First become the leader
	err = election1.Campaign(ctx)
	require.NoError(t, err)

	// Test resigning leadership
	err = election1.Resign()
	require.NoError(t, err)

	// Verify other nodes can become the leader
	election2, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)
	defer election2.Close()

	err = election2.Campaign(ctx)
	require.NoError(t, err)
}

func TestClose(t *testing.T) {
	etcd, clientURL, _ := newEmbedEtcd(t)
	defer func() {
		etcd.Close()
		os.RemoveAll(etcd.Config().Dir)
	}()

	cli := newTestClient(t, clientURL)
	defer cli.Close()

	ctx := context.Background()
	election, err := newEtcdElection(ctx, cli)
	require.NoError(t, err)

	// Test closing
	err = election.Close()
	require.NoError(t, err)
}
