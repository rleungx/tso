package storage

import (
	"io"
	"os/exec"
	"testing"
	"time"

	"github.com/hashicorp/consul/sdk/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/hashicorp/consul/sdk/freeport.checkFreedPorts"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestConsulClient(t *testing.T) {
	// Skip if consul is not installed
	_, err := exec.LookPath("consul")
	if err != nil {
		t.Skip("consul not found, skipping integration test")
	}

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
	client, err := NewConsulClient(srv.HTTPAddr)
	assert.NoError(t, err)

	// Test saving timestamp
	now := time.Now()
	err = client.SaveTimestamp(now)
	assert.NoError(t, err)

	// Test loading timestamp
	loadedTime, err := client.LoadTimestamp()
	assert.NoError(t, err)
	assert.WithinDuration(t, now, loadedTime, time.Second)

	// Test closing client
	err = client.Close()
	assert.NoError(t, err)
}
