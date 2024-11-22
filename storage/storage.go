package storage

import (
	"time"
)

type Storage interface {
	Close() error

	SaveTimestamp(ts time.Time) error
	LoadTimestamp() (time.Time, error)
}
