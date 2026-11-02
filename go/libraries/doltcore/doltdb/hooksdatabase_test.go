// Copyright 2026 Dolthub, Inc.
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

package doltdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/types"
)

type closeCountingHook struct {
	closes int
}

func (h *closeCountingHook) Execute(context.Context, datas.Dataset, *DoltDB) (func(context.Context) error, error) {
	return nil, nil
}
func (h *closeCountingHook) ExecuteForWorkingSets() bool  { return false }
func (h *closeCountingHook) ExecuteForReplicaWrite() bool { return false }
func (h *closeCountingHook) Close() error {
	h.closes++
	return nil
}

func TestDoltDBCloseClosesCommitHooks(t *testing.T) {
	ctx := context.Background()
	ddb, err := LoadDoltDB(ctx, types.Format_DOLT, InMemDoltDB, filesys.LocalFS)
	require.NoError(t, err)
	closeable := &closeCountingHook{}
	ddb.PrependCommitHooks(ctx, closeable)

	require.NoError(t, ddb.Close())
	require.NoError(t, ddb.Close())
	assert.Equal(t, 1, closeable.closes)
	assert.Empty(t, ddb.db.PostCommitHooks())
}
