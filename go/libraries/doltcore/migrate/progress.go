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

package migrate

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/store/hash"
)

type ChunkMapping interface {
	Has(ctx context.Context, addr hash.Hash) (bool, error)
	Get(ctx context.Context, addr hash.Hash) (hash.Hash, error)
	Put(ctx context.Context, old, new hash.Hash) error
}

type CommitStack interface {
	Push(ctx context.Context, cm *doltdb.Commit) error
	Pop(ctx context.Context) (*doltdb.Commit, error)
}

type Progress interface {
	ChunkMapping
	CommitStack

	Log(ctx context.Context, format string, args ...any)
}

type memoryProgress struct {
	stack   []*doltdb.Commit
	mapping map[hash.Hash]hash.Hash
}

func newProgress() Progress {
	return &memoryProgress{
		stack:   make([]*doltdb.Commit, 0, 128),
		mapping: make(map[hash.Hash]hash.Hash, 128),
	}
}

func (mem *memoryProgress) Has(ctx context.Context, addr hash.Hash) (ok bool, err error) {
	_, ok = mem.mapping[addr]
	return
}

func (mem *memoryProgress) Get(ctx context.Context, old hash.Hash) (new hash.Hash, err error) {
	new = mem.mapping[old]
	return
}

func (mem *memoryProgress) Put(ctx context.Context, old, new hash.Hash) (err error) {
	mem.mapping[old] = new
	return
}

func (mem *memoryProgress) Push(ctx context.Context, cm *doltdb.Commit) (err error) {
	mem.stack = append(mem.stack, cm)
	return
}

func (mem *memoryProgress) Pop(ctx context.Context) (cm *doltdb.Commit, err error) {
	if len(mem.stack) == 0 {
		return nil, nil
	}
	top := len(mem.stack) - 1
	cm = mem.stack[top]
	mem.stack = mem.stack[:top]
	return
}

func (mem *memoryProgress) Log(ctx context.Context, format string, args ...any) {
	cli.Println(fmt.Sprintf(format, args...))
}
