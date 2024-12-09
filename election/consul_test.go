package election

import (
	"context"
	"io"
	"os/exec"
	"testing"
	"time"

	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/rleungx/tso/storage"
	"github.com/stretchr/testify/assert"
)

func TestConsulCampaign(t *testing.T) {
	// Skip if consul is not installed
	_, err := exec.LookPath("consul")
	if err != nil {
		t.Skip("consul not found, skipping integration test")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start a test Consul server
	srv, err := testutil.NewTestServerConfigT(t,
		func(c *testutil.TestServerConfig) {
			c.Stdout = io.Discard
			c.Stderr = io.Discard
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	// Create a new ConsulClient instance
	client1, err := storage.NewConsulClient(srv.HTTPAddr)
	assert.NoError(t, err)
	defer client1.Close()

	e1, err := newConsulElection(ctx, client1.Client, "test-id1")
	assert.NoError(t, err)
	e1.(*consulElection).SetFn(func() error {
		<-e1.(*consulElection).ctx.Done()
		return nil
	})
	time.Sleep(100 * time.Millisecond)
	assert.True(t, e1.IsActive())

	// Create a new ConsulClient instance
	client2, err := storage.NewConsulClient(srv.HTTPAddr)
	assert.NoError(t, err)
	defer client2.Close()

	e2, err := newConsulElection(ctx, client2.Client, "test-id2")
	assert.NoError(t, err)
	e2.(*consulElection).SetFn(func() error {
		<-e2.(*consulElection).ctx.Done()
		return nil
	})
	defer e2.Close()
	assert.False(t, e2.IsActive())

	err = e1.Close()
	assert.NoError(t, err)
	time.Sleep(time.Second)
	assert.True(t, e2.IsActive())
}
