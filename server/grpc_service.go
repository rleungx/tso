package server

import (
	"io"
	"time"

	"github.com/rleungx/tso/logger"
	"github.com/rleungx/tso/proto"
	"github.com/rleungx/tso/tso"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetTimestamp implements the GetTimestamp method of the TSO service
func (s *Server) GetTimestamp(reqStream proto.TSO_GetTimestampServer) error {
	if !s.election.IsActive() {
		return status.Error(codes.Unavailable, "server is not active")
	}

	const (
		maxRetries = 3
		retryDelay = 100 * time.Millisecond
	)

	for {
		req, err := reqStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("failed to receive request", zap.Error(err))
			return status.Error(codes.Internal, "failed to receive request")
		}

		// Add retry logic
		var physical, logical int64
		var retryErr error
		for retry := 0; retry < maxRetries; retry++ {
			physical, logical, retryErr = s.timestampOracle.GenerateTimestamp(reqStream.Context(), req.GetCount())
			if retryErr == nil {
				break
			}

			if retryErr == tso.ErrLogicalOverflow {
				// For logical clock overflow, wait for a while and retry
				logger.Warn("logical clock overflow, retrying",
					zap.Int("retry", retry+1),
					zap.Error(retryErr))
				time.Sleep(retryDelay)
				continue
			}

			// Return other errors directly
			logger.Error("failed to generate timestamp", zap.Error(retryErr))
			return status.Error(codes.Internal, "failed to generate timestamp")
		}

		// If it still fails after retries
		if retryErr != nil {
			logger.Error("failed to generate timestamp after retries",
				zap.Int("maxRetries", maxRetries),
				zap.Error(retryErr))
			return status.Error(codes.ResourceExhausted, "failed to generate timestamp after retries")
		}

		resp := &proto.GetTimestampResponse{
			Count: req.Count,
			Timestamp: &proto.Timestamp{
				Physical: physical,
				Logical:  logical,
			},
		}

		if err := reqStream.Send(resp); err != nil {
			logger.Error("failed to send response", zap.Error(err))
			return status.Error(codes.Internal, "failed to send response")
		}
	}
	return nil
}
