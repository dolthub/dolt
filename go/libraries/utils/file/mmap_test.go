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

package file

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMmapOffsets(t *testing.T) {
	data := make([]byte, 256<<10)
	rand.New(rand.NewSource(0)).Read(data)
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, data, 0644))
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	const length = 1024
	// Page aligned and unaligned offsets which are not multiples of Windows' 64KB allocation granularity.
	for _, off := range []int64{0, 13, 4096, 4096 + 13, 64<<10 + 4096, 200<<10 + 7} {
		m, err := Mmap(f, off, length)
		require.NoError(t, err, "offset %d", off)
		got := make([]byte, length)
		_, err = m.ReadAt(got, 0)
		require.NoError(t, err)
		require.Equal(t, data[off:off+length], got, "offset %d", off)
		require.NoError(t, m.Close())
	}
}
