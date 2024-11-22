package main

import (
	"context"
	"os"
	"time"

	"github.com/rleungx/tso/client"
	"github.com/rleungx/tso/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	if err := logger.Init(nil); err != nil {
		os.Exit(1)
	}
	defer logger.Sync()
	conn, err := grpc.DialContext(context.Background(), "localhost:8080", grpc.WithInsecure())
	if err != nil {
		logger.Fatal("Failed to connect to server", zap.Error(err))
	}
	defer conn.Close()

	// Create client, set batch size and max wait time
	client := client.NewTSOClient(conn,
		client.WithBatchSize(100),
		client.WithMaxWait(time.Millisecond*10),
	)
	defer client.Close()

	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		ts, err := client.GetTimestamp(ctx)
		if err != nil {
			logger.Error("Failed to get timestamp", zap.Error(err))
			return
		}
		logger.Info("Received timestamp", zap.Int64("timestamp", ts.Physical), zap.Int64("logical", ts.Logical))
	}
}
