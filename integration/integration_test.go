package integration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rleungx/tso/client"
	"github.com/rleungx/tso/config"
	"github.com/rleungx/tso/logger"
	"github.com/rleungx/tso/proto"
	"github.com/rleungx/tso/server"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/embed"
)

func startEmbeddedEtcd(t *testing.T) (*embed.Etcd, error) {
	tmpDir, err := os.MkdirTemp("", "tso-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := embed.NewConfig()
	cfg.Dir = tmpDir

	lcurl, _ := url.Parse("http://localhost:0")
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}

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
		time.Sleep(100 * time.Millisecond)
	case <-time.After(5 * time.Second):
		e.Close()
		return nil, fmt.Errorf("etcd took too long to start")
	}

	return e, nil
}

func startTSOServer(etcdAddr string, port int) (*server.Server, error) {
	cfg := &config.Config{
		Host:           "localhost",
		Port:           port,
		Backend:        "etcd",
		BackendAddress: etcdAddr,
		Logger: &logger.Options{
			Level:         "error",
			EnableConsole: false,
		},
	}

	srv := server.NewServer(cfg)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// Wait for the server to start
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-errCh:
		return nil, err
	default:
		return srv, nil
	}
}

func TestTSOClusterBasic(t *testing.T) {
	// Start etcd
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	etcdAddr := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	// Start three TSO servers
	srv1, err := startTSOServer(etcdAddr, 10001)
	require.NoError(t, err)
	defer srv1.Stop()

	srv2, err := startTSOServer(etcdAddr, 10002)
	require.NoError(t, err)
	defer srv2.Stop()

	srv3, err := startTSOServer(etcdAddr, 10003)
	require.NoError(t, err)
	defer srv3.Stop()

	// Create client
	cli, err := client.NewTSOClient([]string{
		"localhost:10001",
		"localhost:10002",
		"localhost:10003",
	})
	require.NoError(t, err)
	defer cli.Close()

	// Test normal timestamp retrieval
	t.Run("NormalOperation", func(t *testing.T) {
		var lastTS *proto.Timestamp
		for i := 0; i < 10; i++ {
			ts, err := cli.GetTimestamp(context.Background())
			require.NoError(t, err)
			require.NotNil(t, ts)

			if lastTS != nil {
				currTime := ts.Physical*1e6 + ts.Logical
				lastTime := lastTS.Physical*1e6 + lastTS.Logical
				require.Greater(t, currTime, lastTime, "timestamp should be monotonically increasing")
			}
			lastTS = ts
			time.Sleep(10 * time.Millisecond)
		}
	})

	// Test leader failover
	t.Run("LeaderFailover", func(t *testing.T) {
		ts1, err := cli.GetTimestamp(context.Background())
		require.NoError(t, err)

		err = srv1.Stop()
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		ts2, err := cli.GetTimestamp(context.Background())
		require.NoError(t, err)

		time1 := ts1.Physical*1e6 + ts1.Logical
		time2 := ts2.Physical*1e6 + ts2.Logical
		require.Greater(t, time2, time1, "timestamp should be monotonically increasing after failover")
	})
}

func TestTSOMultiClients(t *testing.T) {
	// Start etcd
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	etcdAddr := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	// Start only one TSO server
	srv, err := startTSOServer(etcdAddr, 10001)
	require.NoError(t, err)
	defer srv.Stop()

	const numClients = 10
	const requestsPerClient = 100

	// Channel to collect all timestamps
	timestamps := make(chan int64, numClients*requestsPerClient)
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()

			cli, err := client.NewTSOClient([]string{"localhost:10001"})
			require.NoError(t, err)
			defer cli.Close()

			var lastTS *proto.Timestamp
			for j := 0; j < requestsPerClient; j++ {
				ts, err := cli.GetTimestamp(context.Background())
				require.NoError(t, err)

				if lastTS != nil {
					currTime := ts.Physical*1e6 + ts.Logical
					lastTime := lastTS.Physical*1e6 + lastTS.Logical
					require.Greater(t, currTime, lastTime,
						fmt.Sprintf("client %d: timestamp went backwards", clientID))
				}
				lastTS = ts

				// Send timestamp to channel
				timestamps <- ts.Physical*1e6 + ts.Logical
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Start a goroutine to collect and verify the uniqueness of timestamps
	go func() {
		seen := make(map[int64]bool)
		for i := 0; i < numClients*requestsPerClient; i++ {
			ts := <-timestamps
			require.False(t, seen[ts], fmt.Sprintf("duplicate timestamp found: %d", ts))
			seen[ts] = true
		}
	}()

	wg.Wait()
}

func TestTSOStandbyServer(t *testing.T) {
	// Start etcd
	etcd, err := startEmbeddedEtcd(t)
	require.NoError(t, err)
	defer etcd.Close()

	etcdAddr := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	// Start two TSO servers
	srv1, err := startTSOServer(etcdAddr, 10001)
	require.NoError(t, err)
	defer srv1.Stop()

	srv2, err := startTSOServer(etcdAddr, 10002)
	require.NoError(t, err)
	defer srv2.Stop()

	// Wait for election to complete
	time.Sleep(time.Second)

	// Create two clients, each connecting to a different server
	cli1, err := client.NewTSOClient([]string{"localhost:10001"})
	require.NoError(t, err)
	defer cli1.Close()

	cli2, err := client.NewTSOClient([]string{"localhost:10002"})
	require.NoError(t, err)
	defer cli2.Close()

	// Test sending requests to both nodes
	for i := 0; i < 10; i++ {
		// Send request to the first node
		ts1, err1 := cli1.GetTimestamp(context.Background())

		// Send request to the second node
		ts2, err2 := cli2.GetTimestamp(context.Background())

		// At least one node should respond successfully
		require.True(t, err1 == nil || err2 == nil, "at least one server should be active")

		// If both succeed, timestamps should be monotonically increasing
		if err1 == nil && err2 == nil {
			time1 := ts1.Physical*1e6 + ts1.Logical
			time2 := ts2.Physical*1e6 + ts2.Logical
			require.NotEqual(t, time1, time2, "timestamps should be unique")
		}

		// Verify standby node returns error
		if err1 != nil {
			require.Contains(t, err1.Error(), "not active",
				"standby node should return not active error")
		}
		if err2 != nil {
			require.Contains(t, err2.Error(), "not active",
				"standby node should return not active error")
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Stop the current leader and verify role switch
	// First, find out which is the leader
	var leaderCli, standbyCli *client.TSOClient
	if _, err1 := cli1.GetTimestamp(context.Background()); err1 == nil {
		leaderCli = cli1
		standbyCli = cli2
	} else {
		leaderCli = cli2
		standbyCli = cli1
	}

	// Get a timestamp as a baseline
	ts, err := leaderCli.GetTimestamp(context.Background())
	require.NoError(t, err)
	baseTime := ts.Physical*1e6 + ts.Logical

	// Stop the current leader
	if cli1 == leaderCli {
		srv1.Stop()
	} else {
		srv2.Stop()
	}

	// Wait for re-election
	time.Sleep(time.Second)

	// Verify the previous standby node can now serve
	ts, err = standbyCli.GetTimestamp(context.Background())
	require.NoError(t, err)
	newTime := ts.Physical*1e6 + ts.Logical
	require.Greater(t, newTime, baseTime, "timestamp should be greater after failover")
}
