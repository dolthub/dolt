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

package commands

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/store/types"
)

func TestBareDBCacheOpensInRequestedFormat(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "bare")
	require.NoError(t, os.MkdirAll(dir, os.ModePerm))

	cache := &bareDBCache{path: dir}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the repository opens on first use")

	const toy = "__TOY__"
	cs, err := cache.Get(ctx, "", toy)
	require.NoError(t, err)
	defer cs.Close()
	assert.Equal(t, toy, cs.Version())
	again, err := cache.Get(ctx, "", types.Format_DOLT.VersionString())
	require.NoError(t, err)
	assert.Same(t, cs, again)
	var _ remotesrv.DBCache = cache
}
