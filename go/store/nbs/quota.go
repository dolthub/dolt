package nbs

import (
	"context"
	"sync"
)

type MemoryQuotaProvider interface {
	AcquireQuota(ctx context.Context, memory uint64) error
	ReleaseQuota(memory uint64) error
	Usage() uint64
}

type UnlimitedQuotaProvider struct {
	mu   sync.Mutex
	used uint64
}

func NewUnlimitedMemQuotaProvider() *UnlimitedQuotaProvider {
	return &UnlimitedQuotaProvider{}
}

func (q *UnlimitedQuotaProvider) AcquireQuota(ctx context.Context, memory uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += memory
	return nil
}

func (q *UnlimitedQuotaProvider) ReleaseQuota(memory uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if memory > q.used {
		panic("tried to release too much quota")
	}
	q.used -= memory
	return nil
}

func (q *UnlimitedQuotaProvider) Usage() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}
