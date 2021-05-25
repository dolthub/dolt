package remotestorage

import (
	"time"
)

type StatsRecorder interface {
	RecordTimeToFirstByte(hedge, retry int, size uint64, d time.Duration)
	RecordDownloadAttemptStart(hedge, retry int, offset, size uint64)
	RecordDownloadComplete(hedge, retry int, size uint64, d time.Duration)
}

var _ StatsRecorder = NullStatsRecorder{}

type NullStatsRecorder struct {
}

func (NullStatsRecorder) RecordTimeToFirstByte(hedge, retry int, size uint64, d time.Duration) {
}

func (NullStatsRecorder) RecordDownloadAttemptStart(hedge, retry int, offset, size uint64) {
}

func (NullStatsRecorder) RecordDownloadComplete(hedge, retry int, size uint64, d time.Duration) {
}
