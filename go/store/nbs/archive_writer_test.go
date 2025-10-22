// Copyright 2025 Dolthub, Inc.
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
	"crypto/rand"
	"io"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
)

func CountFilesInDir(t *testing.T, path string) int {
	cnt := 0
	err := fs.WalkDir(os.DirFS(path), ".", func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		cnt += 1
		return nil
	})
	require.NoError(t, err)
	return cnt
}

func TestArchiveStreamWriterRemove(t *testing.T) {
	t.Run("RemoveOnFinishedWriterRemovesFile", func(t *testing.T) {
		dir := t.TempDir()
		require.Equal(t, 1, CountFilesInDir(t, dir))
		asw, err := NewArchiveStreamWriter(dir)
		require.NoError(t, err)
		contents := make([]byte, 1024)
		_, err = io.ReadFull(rand.Reader, contents)
		require.NoError(t, err)
		_, err = asw.AddChunk(ChunkToCompressedChunk(chunks.NewChunk(contents)))
		require.NoError(t, err)
		_, _, err = asw.Finish()
		require.NoError(t, err)
		rdr, err := asw.Reader()
		require.NoError(t, err)
		require.NoError(t, rdr.Close())
		err = asw.Remove()
		require.NoError(t, err)
		require.Equal(t, 1, CountFilesInDir(t, dir))
	})
	t.Run("CancelOnWriterRemovesFile", func(t *testing.T) {
		dir := t.TempDir()
		require.Equal(t, 1, CountFilesInDir(t, dir))
		asw, err := NewArchiveStreamWriter(dir)
		require.NoError(t, err)
		contents := make([]byte, 1024)
		_, err = io.ReadFull(rand.Reader, contents)
		require.NoError(t, err)
		_, err = asw.AddChunk(ChunkToCompressedChunk(chunks.NewChunk(contents)))
		require.NoError(t, err)
		err = asw.Cancel()
		require.NoError(t, err)
		require.Equal(t, 1, CountFilesInDir(t, dir))
	})
}
