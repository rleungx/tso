package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rleungx/tso/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// Mock TSO server
type mockTSOServer struct {
	proto.UnimplementedTSOServer
	physical int64
	logical  int64
}

func (s *mockTSOServer) GetTimestamp(stream proto.TSO_GetTimestampServer) error {
	_, err := stream.Recv()
	if err != nil {
		return err
	}

	resp := &proto.GetTimestampResponse{
		Timestamp: &proto.Timestamp{
			Physical: s.physical,
			Logical:  s.logical,
		},
	}
	return stream.Send(resp)
}

func TestNewTSOClient(t *testing.T) {
	// Set up mock server
	endpoint, cleanup := setupMockServer(t)
	defer cleanup()

	tests := []struct {
		name      string
		endpoints []string
		wantErr   bool
	}{
		{
			name:      "Empty endpoints",
			endpoints: []string{},
			wantErr:   true,
		},
		{
			name:      "Valid endpoints",
			endpoints: []string{endpoint},
			wantErr:   false,
		},
		{
			name:      "Invalid endpoints",
			endpoints: []string{"invalid-address:1234"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTSOClient(tt.endpoints)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				client.Close()
			}
		})
	}
}

func TestTSOClientOptions(t *testing.T) {
	// Set up mock server
	endpoint, cleanup := setupMockServer(t)
	defer cleanup()

	client, err := NewTSOClient(
		[]string{endpoint}, // Use real mock server address
		WithMaxBatchSize(200),
		WithMaxWaitTime(time.Millisecond*20),
	)
	require.NoError(t, err)
	if client != nil {
		defer client.Close()

		// Verify if options are set correctly
		require.Equal(t, uint32(200), client.options.maxBatchSize)
		require.Equal(t, time.Millisecond*20, client.options.maxWaitTime)
	} else {
		t.Fatal("client should not be nil")
	}
}

func setupMockServer(t *testing.T) (string, func()) {
	mock := &mockTSOServer{
		physical: time.Now().UnixNano() / int64(time.Millisecond),
		logical:  0,
	}

	server := grpc.NewServer()
	proto.RegisterTSOServer(server, mock)

	// Start a listener on a random port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("mock server exit: %v", err)
		}
	}()

	return lis.Addr().String(), func() {
		server.Stop()
		lis.Close()
	}
}

func TestGetTimestamp(t *testing.T) {
	// Set up mock server
	endpoint, cleanup := setupMockServer(t)
	defer cleanup()

	// Create client using real server address
	client, err := NewTSOClient([]string{endpoint})
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ts, err := client.GetTimestamp(ctx)
	require.NoError(t, err)
	physical, logical, err := ts.Wait()
	require.NoError(t, err)
	require.NotNil(t, physical)
	require.NotNil(t, logical)
	require.Greater(t, physical, int64(0))
}

func TestGetTimestampContextCancel(t *testing.T) {
	// Set up mock server
	endpoint, cleanup := setupMockServer(t)
	defer cleanup()

	client, err := NewTSOClient([]string{endpoint})
	require.NoError(t, err)
	if client == nil {
		t.Fatal("client should not be nil")
		return
	}
	defer client.Close()

	// Wait a short time to ensure connection is established
	time.Sleep(time.Millisecond * 100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	ts, err := client.GetTimestamp(ctx)
	if err == nil {
		physical, logical, err := ts.Wait()
		require.Error(t, err)
		require.Equal(t, int64(0), physical)
		require.Equal(t, int64(0), logical)
	} else {
		require.Nil(t, ts)
	}
}

func TestSwitchEndpoint(t *testing.T) {
	// Set up two mock servers
	endpoint1, cleanup1 := setupMockServer(t)
	defer cleanup1()
	endpoint2, cleanup2 := setupMockServer(t)
	defer cleanup2()
	endpoint3, cleanup3 := setupMockServer(t)
	defer cleanup3()

	endpoints := []string{
		endpoint1,
		endpoint2,
		endpoint3,
	}

	client, err := NewTSOClient(endpoints)
	require.NoError(t, err)
	if client == nil {
		t.Fatal("client should not be nil")
		return
	}
	defer client.Close()

	// Wait a short time to ensure initial connection is established
	time.Sleep(time.Millisecond * 100)

	// Test switching nodes
	err = client.conn.switchToNextEndpoint()
	require.NoError(t, err)

	// Verify that timestamp can be obtained
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ts, err := client.GetTimestamp(ctx)
	require.NoError(t, err)
	require.NotNil(t, ts)
}
