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

package datas

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/types"
)

// recordingSafepointController tracks which methods were called and in what order.
type recordingSafepointController struct {
	calls  []string
	keeper func(hash.Hash) bool
}

var _ types.GCSafepointController = (*recordingSafepointController)(nil)

func (r *recordingSafepointController) BeginGC(_ context.Context, keeper func(hash.Hash) bool) error {
	r.calls = append(r.calls, "BeginGC")
	r.keeper = keeper
	return nil
}
func (r *recordingSafepointController) EstablishPreFinalizeSafepoint(context.Context) error {
	r.calls = append(r.calls, "PreFinalize")
	return nil
}
func (r *recordingSafepointController) EstablishPostFinalizeSafepoint(context.Context) error {
	r.calls = append(r.calls, "PostFinalize")
	return nil
}
func (r *recordingSafepointController) CancelSafepoint() {
	r.calls = append(r.calls, "Cancel")
}

func TestCollectGarbageCollectsUnreferencedChunks(t *testing.T) {
	ctx := context.Background()
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	vrw := db.(*database).ValueStore

	// Create a dataset with a committed value (reachable).
	ds, err := db.GetDataset(ctx, "refs/heads/main")
	require.NoError(t, err)
	ds, err = CommitValue(ctx, db, ds, types.String("keep"))
	require.NoError(t, err)

	// Write an unreferenced value directly to the value store.
	unreferencedRef, err := vrw.WriteValue(ctx, types.String("discard"))
	require.NoError(t, err)
	unreferencedHash := unreferencedRef.TargetHash()

	// Confirm both are readable before GC.
	val, err := vrw.ReadValue(ctx, unreferencedHash)
	require.NoError(t, err)
	assert.NotNil(t, val)

	headAddr, hasHead := ds.MaybeHeadAddr()
	require.True(t, hasHead)
	val, err = vrw.ReadValue(ctx, headAddr)
	require.NoError(t, err)
	assert.NotNil(t, val)

	// Run GC with nil classifier (all newGen).
	sc := &recordingSafepointController{}
	gcConfig := chunks.GCConfig{
		Mode:                chunks.GCMode_Full,
		ArchiveLevel:        chunks.NoArchive,
		IncrementalFileSize: chunks.IncrementalGCTablesDisabled,
	}
	err = CollectGarbage(ctx, db, gcConfig, nil, sc)
	require.NoError(t, err)
	vrw.PurgeCaches()

	// Verify safepoint controller was called in the correct order.
	require.True(t, len(sc.calls) >= 3, "expected at least 3 safepoint calls, got %v", sc.calls)
	assert.Equal(t, "BeginGC", sc.calls[0])
	assert.Equal(t, "PreFinalize", sc.calls[1])
	assert.Equal(t, "PostFinalize", sc.calls[2])

	// Committed value still reachable.
	val, err = vrw.ReadValue(ctx, headAddr)
	require.NoError(t, err)
	assert.NotNil(t, val)

	// Unreferenced value collected.
	val, err = vrw.ReadValue(ctx, unreferencedHash)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestCollectGarbageWithRefClassifier(t *testing.T) {
	ctx := context.Background()
	storage := &chunks.TestStorage{}
	db := NewDatabase(storage.NewView())
	defer db.Close()

	vrw := db.(*database).ValueStore

	ds1, err := db.GetDataset(ctx, "refs/heads/main")
	require.NoError(t, err)
	ds1, err = CommitValue(ctx, db, ds1, types.String("branch-data"))
	require.NoError(t, err)

	ds2, err := db.GetDataset(ctx, "refs/tags/v1.0")
	require.NoError(t, err)
	ds2, err = CommitValue(ctx, db, ds2, types.String("tag-data"))
	require.NoError(t, err)

	// Only "refs/heads/" are old gen.
	classifier := func(datasetID string) bool {
		return len(datasetID) > 11 && datasetID[:11] == "refs/heads/"
	}

	sc := &recordingSafepointController{}
	gcConfig := chunks.GCConfig{
		Mode:                chunks.GCMode_Full,
		ArchiveLevel:        chunks.NoArchive,
		IncrementalFileSize: chunks.IncrementalGCTablesDisabled,
	}
	err = CollectGarbage(ctx, db, gcConfig, classifier, sc)
	require.NoError(t, err)
	vrw.PurgeCaches()

	// Verify safepoint controller was exercised.
	assert.Contains(t, sc.calls, "BeginGC")
	assert.Contains(t, sc.calls, "PreFinalize")
	assert.Contains(t, sc.calls, "PostFinalize")

	// Both datasets still reachable after GC.
	headAddr1, hasHead := ds1.MaybeHeadAddr()
	require.True(t, hasHead)
	val, err := vrw.ReadValue(ctx, headAddr1)
	require.NoError(t, err)
	assert.NotNil(t, val)

	headAddr2, hasHead := ds2.MaybeHeadAddr()
	require.True(t, hasHead)
	val, err = vrw.ReadValue(ctx, headAddr2)
	require.NoError(t, err)
	assert.NotNil(t, val)
}
