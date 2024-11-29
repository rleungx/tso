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
	tsoClient, err := client.NewTSOClient([]string{"127.0.0.1:7788"},
		client.WithMaxBatchSize(100),
		client.WithMaxWaitTime(time.Millisecond*10),
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
		physical, logical, err := ts.Wait()
		if err != nil {
			logger.Error("Failed to wait timestamp", zap.Error(err))
			return
		}
		logger.Info("Received timestamp", zap.Int64("timestamp", physical), zap.Int64("logical", logical))
	}
}
