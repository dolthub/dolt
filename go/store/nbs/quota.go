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

type noopQuotaProvider struct {
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

func (q *noopQuotaProvider) AcquireQuota(ctx context.Context, memory uint64) error {
	return nil
}

func (q *noopQuotaProvider) ReleaseQuota(memory uint64) error {
	return nil
}

func (q *noopQuotaProvider) Usage() uint64 {
	return 0
}
