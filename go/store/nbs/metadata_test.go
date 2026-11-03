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
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestGetStorageMetadataClosesFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	newGen := filepath.Join(dir, ".dolt", "noms")
	oldGen := filepath.Join(newGen, "oldgen")
	require.NoError(t, os.MkdirAll(oldGen, 0777))

	tbl, err := writeTableData(newGen, []byte("hello"), []byte("goodbye"))
	require.NoError(t, err)
	writeTestManifest(t, newGen, tableSpec{name: tbl, chunkCount: 2})
	writeTestManifest(t, oldGen, writeTestArchive(t, oldGen))

	before := openFileCount()
	for _, mmap := range []bool{false, true} {
		smd, err := GetStorageMetadata(ctx, dir, &Stats{}, mmap)
		require.NoError(t, err)
		require.Len(t, smd.artifacts, 2)
	}
	// Where files cannot be counted, removing |dir| at cleanup fails while they are open.
	if before >= 0 {
		require.LessOrEqual(t, openFileCount(), before)
	}
	require.False(t, mapsFileUnder(dir))
}

func writeTestManifest(t *testing.T, dir string, specs ...tableSpec) {
	f, err := os.Create(filepath.Join(dir, manifestFileName))
	require.NoError(t, err)
	require.NoError(t, writeManifest(f, manifestContents{nbfVers: constants.FormatDefaultString, lock: hash.Of([]byte("lock")), specs: specs}))
	require.NoError(t, f.Close())
}

func writeTestArchive(t *testing.T, dir string) tableSpec {
	sink := NewFixedBufferByteSink(make([]byte, 4096))
	aw := newArchiveWriterWithSink(sink)
	cc := ChunkToCompressedChunk(chunks.NewChunk([]byte("archived")))
	bsId, err := aw.writeByteSpan(cc.FullCompressedChunk)
	require.NoError(t, err)
	require.NoError(t, aw.stageSnappyChunk(cc.H, bsId))
	require.NoError(t, aw.finalizeByteSpans())
	require.NoError(t, aw.writeIndex())
	require.NoError(t, aw.writeMetadata([]byte("{}")))
	require.NoError(t, aw.writeFooter())
	name, err := aw.getName()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name.String()+ArchiveFileSuffix), sink.buff[:sink.pos], 0666))
	return tableSpec{name: name, chunkCount: 1}
}

// openFileCount returns the number of files this process has open, or -1 where it cannot tell.
func openFileCount() int {
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

// mapsFileUnder reports whether this process maps a file under |dir|, or false where it cannot tell.
func mapsFileUnder(dir string) bool {
	maps, err := os.ReadFile("/proc/self/maps")
	return err == nil && bytes.Contains(maps, []byte(dir))
}
