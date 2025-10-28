// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nbs

import (
	"context"
	"sync"
)

type MemoryQuotaProvider interface {
	AcquireQuotaBytes(ctx context.Context, sz int) ([]byte, error)
	AcquireQuotaUint64s(ctx context.Context, sz int) ([]uint64, error)
	AcquireQuotaUint32s(ctx context.Context, sz int) ([]uint32, error)
	ReleaseQuotaBytes(sz int)
	Usage() uint64
}

type UnlimitedQuotaProvider struct {
	mu   sync.Mutex
	used uint64
}

func NewUnlimitedMemQuotaProvider() *UnlimitedQuotaProvider {
	return &UnlimitedQuotaProvider{}
}

func (q *UnlimitedQuotaProvider) AcquireQuotaBytes(ctx context.Context, sz int) ([]byte, error) {
	buf := make([]byte, sz)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += uint64(sz)
	return buf, nil
}

func (q *UnlimitedQuotaProvider) AcquireQuotaUint32s(ctx context.Context, sz int) ([]uint32, error) {
	buf := make([]uint32, sz)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += uint64(sz) * uint32Size
	return buf, nil
}

func (q *UnlimitedQuotaProvider) AcquireQuotaUint64s(ctx context.Context, sz int) ([]uint64, error) {
	buf := make([]uint64, sz)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.used += uint64(sz) * uint64Size
	return buf, nil
}

func (q *UnlimitedQuotaProvider) ReleaseQuotaBytes(sz int) {
	if sz < 0 {
		panic("tried to release negative bytes")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if uint64(sz) > q.used {
		panic("tried to release too much quota")
	}
	q.used -= uint64(sz)
}

func (q *UnlimitedQuotaProvider) Usage() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.used
}
