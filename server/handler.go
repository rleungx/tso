package server

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Timestamp struct {
	Physical int64 `json:"physical,omitempty"`
	Logical  int64 `json:"logical,omitempty"`
}

type TimestampResponse struct {
	Timestamp *Timestamp `json:"timestamp"`
	Count     uint32     `json:"count"`
}

// GetTS handles the timestamp request
func (s *Server) GetTS(c *gin.Context) {
	// Add active status check
	if !s.election.IsActive() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server is not active"})
		return
	}

	countStr := c.Query("count")
	count, err := strconv.ParseUint(countStr, 10, 32) // Convert string to uint32
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid count"})
		return
	}
	physical, logical, err := s.timestampOracle.GenerateTimestamp(c.Request.Context(), uint32(count))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate timestamp", "details": err.Error()})
		return
	}
	// Create response
	response := TimestampResponse{
		Timestamp: &Timestamp{
			Physical: physical,
			Logical:  logical,
		},
		Count: uint32(count), // Assume count is 1
	}

	// Return JSON response
	c.JSON(http.StatusOK, response)
}
