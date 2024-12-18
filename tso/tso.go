package tso

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rleungx/tso/logger"
	"github.com/rleungx/tso/storage"
	"go.uber.org/zap"
)

const (
	// updateTimestampGuard is the minimum timestamp interval.
	updateTimestampGuard = time.Millisecond
	// tsoUpdatePhysicalInterval is the interval to update the physical part of a tso.
	tsoUpdatePhysicalInterval = 50 * time.Millisecond
)

// ZeroTime is a zero time.
var (
	ZeroTime   = time.Time{}
	maxLogical = int64(1 << 18)
)

var ErrLogicalOverflow = errors.New("logical clock overflow")

// TimestampOracle
type TimestampOracle struct {
	sync.RWMutex
	ctx           context.Context
	physical      time.Time
	logical       int64
	lastSavedTime atomic.Value // stored as time.Time
	storage       storage.Storage
}

// NewTimestampOracle creates a new TimestampOracle instance
func NewTimestampOracle(ctx context.Context, storage storage.Storage) *TimestampOracle {
	tso := &TimestampOracle{
		ctx:           ctx,
		storage:       storage,
		physical:      ZeroTime,
		logical:       0,
		lastSavedTime: atomic.Value{},
	}
	return tso
}

func (ts *TimestampOracle) UpdateTimestampLoop() error {
	defer ts.reset()
	if err := ts.syncTimestamp(ts.storage); err != nil {
		logger.Error("failed to sync timestamp", zap.Error(err))
		return err
	}

	ticker := time.NewTicker(tsoUpdatePhysicalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ts.ctx.Done():
			return nil
		case <-ticker.C:
			if err := ts.updateTimestamp(ts.storage); err != nil {
				logger.Error("failed to update timestamp", zap.Error(err))
				return err
			}
		}
	}
}

func (ts *TimestampOracle) get() (time.Time, int64) {
	ts.RLock()
	defer ts.RUnlock()
	if ts.physical == ZeroTime {
		return ZeroTime, 0
	}
	return ts.physical, ts.logical
}

func (t *TimestampOracle) setPhysical(next time.Time, force bool) {
	t.Lock()
	defer t.Unlock()
	// Do not update the zero physical time if the `force` flag is false.
	if t.physical == ZeroTime && !force {
		return
	}
	// make sure the ts won't fall back
	if subTSOPhysicalByWallClock(next, t.physical) > 0 {
		t.physical = next
		t.logical = 0
	}
}

// GenerateTimestamp is used to generate a timestamp.
func (t *TimestampOracle) GenerateTimestamp(ctx context.Context, count uint32) (physical int64, logical int64, err error) {
	t.Lock()
	defer t.Unlock()
	if t.physical == ZeroTime {
		return 0, 0, errors.New("timestamp oracle not initialized")
	}

	// Check if it will exceed the maximum value
	if t.logical+int64(count) >= maxLogical {
		return 0, 0, ErrLogicalOverflow
	}

	physical = t.physical.UnixNano() / int64(time.Millisecond)
	t.logical += int64(count)
	logical = t.logical
	return physical, logical, nil
}

// syncTimestamp is used to synchronize the timestamp.
func (ts *TimestampOracle) syncTimestamp(s storage.Storage) error {
	last, err := s.LoadTimestamp()
	if err != nil {
		return err
	}

	next := time.Now()
	// If the current system time minus the saved timestamp is less than `UpdateTimestampGuard`,
	// the timestamp allocation will start from the saved timestamp temporarily.
	if subRealTimeByWallClock(next, last) < updateTimestampGuard {
		next = last.Add(updateTimestampGuard)
	}

	save := next.Add(3 * time.Second)
	if err = s.SaveTimestamp(save); err != nil {
		return err
	}
	ts.lastSavedTime.Store(save)

	// save into memory
	ts.setPhysical(next, true)
	return nil
}

// updateTimestamp is used to update the timestamp.
func (ts *TimestampOracle) updateTimestamp(s storage.Storage) error {
	prevPhysical, prevLogical := ts.get()

	now := time.Now()
	jetLag := subRealTimeByWallClock(now, prevPhysical)

	var next time.Time
	// If the system time is greater, it will be synchronized with the system time.
	if jetLag > updateTimestampGuard {
		next = now
	} else if prevLogical > maxLogical/2 {
		// The reason choosing maxLogical/2 here is that it's big enough for common cases.
		// Because there is enough timestamp can be allocated before next update.
		next = prevPhysical.Add(time.Millisecond)
	} else {
		// It will still use the previous physical time to alloc the timestamp.
		return nil
	}

	// It is not safe to increase the physical time to `next`.
	// The time window needs to be updated and saved to storage.
	if subRealTimeByWallClock(ts.lastSavedTime.Load().(time.Time), next) <= updateTimestampGuard {
		save := next.Add(3 * time.Second)
		if err := s.SaveTimestamp(save); err != nil {
			return err
		}
		ts.lastSavedTime.Store(save)
	}
	// save into memory
	ts.setPhysical(next, false)
	return nil
}

// reset resets the TimestampOracle to its initial state
func (t *TimestampOracle) reset() {
	t.Lock()
	defer t.Unlock()

	t.physical = ZeroTime
	t.logical = 0
	t.lastSavedTime.Store(ZeroTime)
}

// subRealTimeByWallClock returns the duration between two different time.Time structs.
// You should use it to compare the real-world system time.
// And DO NOT USE IT TO COMPARE two TSOs' physical times directly in some cases.
func subRealTimeByWallClock(after, before time.Time) time.Duration {
	return time.Duration(after.UnixNano() - before.UnixNano())
}

// subTSOPhysicalByWallClock returns the duration between two different TSOs' physical times with millisecond precision.
func subTSOPhysicalByWallClock(after, before time.Time) int64 {
	return after.UnixNano()/int64(time.Millisecond) - before.UnixNano()/int64(time.Millisecond)
}
