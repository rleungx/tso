package main

import (
	"context"
	"os"
	"time"

	"github.com/rleungx/tso/client"
	"github.com/rleungx/tso/logger"
	"go.uber.org/zap"
)

func main() {
	if err := logger.Init(nil); err != nil {
		os.Exit(1)
	}
	defer logger.Sync()

	// Create client, set batch size and max wait time
	tsoClient, err := client.NewTSOClient([]string{"localhost:8080"},
		client.WithBatchSize(100),
		client.WithMaxWait(time.Millisecond*10),
	)
	if err != nil {
		logger.Fatal("Failed to create TSO client", zap.Error(err))
	}
	defer tsoClient.Close()

	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ts, err := tsoClient.GetTimestamp(ctx)
		if err != nil {
			logger.Error("Failed to get timestamp", zap.Error(err))
			return
		}
		logger.Info("Received timestamp", zap.Int64("timestamp", ts.Physical), zap.Int64("logical", ts.Logical))
	}
}
