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
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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
		var (
			lastTS            *proto.Timestamp
			physical, logical int64
		)
		for i := 0; i < 10; i++ {
			ts, err := cli.GetTimestamp(context.Background())
			require.NoError(t, err)
			require.NotNil(t, ts)
			physical, logical, err = ts.Wait()
			require.NoError(t, err)
			if lastTS != nil {
				currTime := physical*1e6 + logical
				lastTime := lastTS.Physical*1e6 + lastTS.Logical
				require.Greater(t, currTime, lastTime, "timestamp should be monotonically increasing")
			}
			lastTS = &proto.Timestamp{
				Physical: physical,
				Logical:  logical,
			}
			time.Sleep(10 * time.Millisecond)
		}
	})

	// Test leader failover
	t.Run("LeaderFailover", func(t *testing.T) {
		ts1, err := cli.GetTimestamp(context.Background())
		require.NoError(t, err)
		physical1, logical1, err := ts1.Wait()
		require.NoError(t, err)
		time1 := physical1*1e6 + logical1

		err = srv1.Stop()
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)
		ts2, err := cli.GetTimestamp(context.Background())
		require.NoError(t, err)

		physical2, logical2, err := ts2.Wait()
		require.NoError(t, err)
		time2 := physical2*1e6 + logical2
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
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			cli, err := client.NewTSOClient([]string{"localhost:10001"})
			require.NoError(t, err)
			defer cli.Close()

			var (
				lastTS            *proto.Timestamp
				physical, logical int64
			)
			for j := 0; j < requestsPerClient; j++ {
				ts, err := cli.GetTimestamp(context.Background())
				require.NoError(t, err)
				physical, logical, err = ts.Wait()
				require.NoError(t, err)
				if lastTS != nil {
					currTime := physical*1e6 + logical
					lastTime := lastTS.Physical*1e6 + lastTS.Logical
					require.Greater(t, currTime, lastTime,
						fmt.Sprintf("client %d: timestamp went backwards", clientID))
				}
				lastTS = &proto.Timestamp{
					Physical: physical,
					Logical:  logical,
				}

				// Send timestamp to channel
				timestamps <- physical*1e6 + logical
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Add(1)
	// Start a goroutine to collect and verify the uniqueness of timestamps
	go func() {
		defer wg.Done()

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
		var physical1, logical1 int64
		if err1 == nil {
			physical1, logical1, err1 = ts1.Wait()
		}

		// Send request to the second node
		ts2, err2 := cli2.GetTimestamp(context.Background())
		var physical2, logical2 int64
		if err2 == nil {
			physical2, logical2, err2 = ts2.Wait()
		}

		// At least one node should respond successfully
		require.True(t, err1 == nil || err2 == nil, "at least one server should be active")

		// If both succeed, timestamps should be monotonically increasing
		if err1 == nil && err2 == nil {
			time1 := physical1*1e6 + logical1
			time2 := physical2*1e6 + logical2
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
	ts1, err1 := cli1.GetTimestamp(context.Background())
	_, _, err11 := ts1.Wait()
	if err1 == nil && err11 == nil {
		leaderCli = cli1
		standbyCli = cli2
	} else {
		leaderCli = cli2
		standbyCli = cli1
	}

	// Get a timestamp as a baseline
	ts, err := leaderCli.GetTimestamp(context.Background())
	require.NoError(t, err)
	physical, logical, err := ts.Wait()
	require.NoError(t, err)
	baseTime := physical*1e6 + logical

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
	physical, logical, err = ts.Wait()
	require.NoError(t, err)
	newTime := physical*1e6 + logical
	require.Greater(t, newTime, baseTime, "timestamp should be greater after failover")
}

// Test the impact of different batch sizes
func BenchmarkTSOBatchSize(b *testing.B) {
	etcd, err := startEmbeddedEtcd(&testing.T{})
	require.NoError(b, err)
	defer etcd.Close()
	etcdAddr := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	scenarios := []struct {
		name         string
		maxBatchSize uint32
	}{
		{"Batch1", 1},     // No batching
		{"Batch8", 8},     // Small batch
		{"Batch16", 16},   // Small batch
		{"Batch32", 32},   // Medium batch
		{"Batch64", 64},   // Large batch
		{"Batch128", 128}, // Large batch
		{"Batch256", 256}, // Very large batch
	}
	srv, err := startTSOServer(etcdAddr, 10001)
	require.NoError(b, err)
	defer srv.Stop()

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			cli, err := client.NewTSOClient(
				[]string{"localhost:10001"},
				client.WithMaxBatchSize(sc.maxBatchSize),
			)
			require.NoError(b, err)
			defer cli.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					future, err := cli.GetTimestamp(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					_, _, err = future.Wait()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// Test the impact of different wait times
func BenchmarkTSOMaxWaitTime(b *testing.B) {
	etcd, err := startEmbeddedEtcd(&testing.T{})
	require.NoError(b, err)
	defer etcd.Close()
	etcdAddr := fmt.Sprintf("http://%s", etcd.Clients[0].Addr().String())

	scenarios := []struct {
		name        string
		maxWaitTime time.Duration
	}{
		{"NoWait", 0},
		{"Wait100us", 100 * time.Microsecond}, // Minimum wait time
		{"Wait300us", 300 * time.Microsecond}, // Small wait time
		{"Wait500us", 500 * time.Microsecond}, // Medium wait time
		{"Wait1ms", 1 * time.Millisecond},     // Common wait time
		{"Wait2ms", 2 * time.Millisecond},     // Long wait time
		{"Wait5ms", 5 * time.Millisecond},     // Long wait time
		{"Wait10ms", 10 * time.Millisecond},   // Maximum wait time
	}
	srv, err := startTSOServer(etcdAddr, 10001)
	require.NoError(b, err)
	defer srv.Stop()

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			cli, err := client.NewTSOClient(
				[]string{"localhost:10001"},
				client.WithMaxWaitTime(sc.maxWaitTime),
			)
			require.NoError(b, err)
			defer cli.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					future, err := cli.GetTimestamp(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					_, _, err = future.Wait()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
