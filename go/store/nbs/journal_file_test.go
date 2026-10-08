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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestJournalFile(t *testing.T) (*journalFile, string) {
	path := filepath.Join(t.TempDir(), "journal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return newJournalFile(f), path
}

func fileSize(t *testing.T, path string) int64 {
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

func TestJournalFileWriteAtPreparesAhead(t *testing.T) {
	if journalPadBufferSize == 0 {
		t.Skip("journal padding is disabled on this platform")
	}
	jf, path := newTestJournalFile(t)
	data := []byte("record")
	n, err := jf.writeAt(data, 0)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data))+journalPadBufferSize, jf.preparedThrough)
	assert.Equal(t, jf.preparedThrough, fileSize(t, path))

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, data, contents[:len(data)])
	assert.True(t, bytes.Equal(contents[len(data):], make([]byte, len(contents)-len(data))))

	_, err = jf.writeAt(data, int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, int64(len(data))+journalPadBufferSize, jf.preparedThrough)
}

func TestJournalFileEmptyWritePreparesNothing(t *testing.T) {
	jf, path := newTestJournalFile(t)
	n, err := jf.writeAt(nil, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, int64(0), jf.preparedThrough)
	assert.Equal(t, int64(0), fileSize(t, path))
}

func TestJournalFileFinishTruncatesPreparedSpace(t *testing.T) {
	jf, path := newTestJournalFile(t)
	data := []byte("record")
	_, err := jf.writeAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, jf.finish(int64(len(data))))
	assert.Equal(t, int64(len(data)), fileSize(t, path))
}

func TestJournalFileFinishKeepsUnpreparedBytes(t *testing.T) {
	jf, path := newTestJournalFile(t)
	_, err := jf.f.WriteAt([]byte("records then trailing bytes"), 0)
	require.NoError(t, err)
	require.NoError(t, jf.finish(7))
	assert.Equal(t, int64(len("records then trailing bytes")), fileSize(t, path))
}
