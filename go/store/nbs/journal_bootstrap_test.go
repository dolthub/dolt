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

package nbs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func newTestJournalingStore(t *testing.T, dir string) *NomsBlockStore {
	t.Helper()
	st, err := NewLocalJournalingStore(context.Background(), constants.FormatDefaultString, dir, NewUnlimitedMemQuotaProvider(), false, nil)
	require.NoError(t, err)
	return st
}

// Creating the journal is store-wide work that happens to be triggered by
// whichever commit runs first. A client disconnecting mid-commit must not
// abandon it: the bootstrap is detached from the caller's cancellation.
func TestJournalBootstrapIgnoresCallerCancellation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := newTestJournalingStore(t, dir)

	c := chunks.NewChunk([]byte("hello"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	ok, err := st.Commit(cancelled, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)

	j := st.persister.(*ChunkJournal)
	require.NotNil(t, j.wr, "journal bootstrapped despite the cancelled caller")
	require.False(t, j.contents.lock.IsEmpty(), "contents trued up against the manifest")

	require.NoError(t, st.Close())
}

// A bootstrap that fails after installing the journal writer must roll back,
// not leave the journal permanently half-initialized. |journal.idx| as a
// directory makes loadJournalIndex fail to obtain a writable index handle,
// which is the one fatal case in that path.
func TestJournalBootstrapRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, journalIndexFileName), 0777))

	st := newTestJournalingStore(t, dir)
	c := chunks.NewChunk([]byte("hello"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	_, err := st.Commit(ctx, c.Hash(), hash.Hash{})
	require.Error(t, err)

	j := st.persister.(*ChunkJournal)
	require.Nil(t, j.wr, "journal writer must be rolled back, not left installed")
	_, serr := os.Stat(filepath.Join(dir, chunkJournalName))
	require.True(t, os.IsNotExist(serr), "the journal file this attempt created must be gone")

	// Not bricked: the next commit bootstraps and succeeds.
	ok, err := st.Commit(ctx, c.Hash(), hash.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, j.wr)
	require.False(t, j.contents.lock.IsEmpty())

	require.NoError(t, st.Close())
}

// A brand new database bootstraps its journal before any manifest exists, so
// j.contents is legitimately empty --- no error, nothing to roll back. Closing
// after a failed first commit must not flush those empty contents.
func TestNewDatabaseFailedFirstCommitThenClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st := newTestJournalingStore(t, dir)

	c := chunks.NewChunk([]byte("hello"))
	require.NoError(t, st.Put(ctx, c, noopGetAddrs))
	// Commit a root that is not present: the memtable is persisted to the
	// journal first, then errorIfDangling rejects the root, so manifest.Update
	// is never reached.
	_, err := st.Commit(ctx, hash.Parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), hash.Hash{})
	require.Error(t, err)

	j := st.persister.(*ChunkJournal)
	require.NotNil(t, j.wr, "the journal bootstrapped successfully")
	require.True(t, j.contents.lock.IsEmpty(), "no manifest to true up against")

	require.NoError(t, st.Close())
}
