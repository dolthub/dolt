// Copyright 2024 Dolthub, Inc.
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

package ranges

import (
	"math/rand/v2"
	"testing"

	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
)

func TestBTree(t *testing.T) {
	t.Run("MakeOne", func(t *testing.T) {
		tree := btree.NewOrderedG[int](64)
		for i := 0; i < 4096; i++ {
			tree.ReplaceOrInsert(i)
		}
		assert.Equal(t, tree.Len(), 4096)
	})
	t.Run("DeleteFromOne", func(t *testing.T) {
		tree := btree.NewOrderedG[int](64)
		for i := 0; i < 4096; i++ {
			tree.ReplaceOrInsert(i)
		}
		reader := tree.Clone()
		reader.Ascend(func(i int) bool {
			if i%2 == 0 {
				tree.Delete(i)
			}
			return true
		})
		assert.Equal(t, tree.Len(), 2048)
	})
}

func TestTree(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		tree := NewTree(8 * 1024)
		assert.NotNil(t, tree)
	})
	t.Run("OneRange", func(t *testing.T) {
		assertTree := func(t *testing.T, tree *Tree) {
			min, _ := tree.t.Min()
			max, _ := tree.t.Max()
			assert.Equal(t, &max.Url, &min.Url)
			assert.Equal(t, max.Region, min.Region)
			i := 0
			tree.t.Ascend(func(gr *GetRange) bool {
				assert.Equal(t, &gr.Url, &min.Url, "%v at %d", gr, i)
				assert.Equal(t, gr.Region, min.Region, "%v at %d", gr, i)
				i += 1
				return true
			})
			assert.Equal(t, 32, i)

			assert.Equal(t, min.Region.StartOffset, uint64(0))
			assert.Equal(t, min.Region.EndOffset, uint64(15*16*1024+8*1024+1024))
			assert.Equal(t, min.Region.MatchedBytes, uint64(32768))

			assert.Equal(t, 1, tree.regions.Len())
		}

		t.Run("AscendingThenDescending", func(t *testing.T) {
			tree := NewTree(8 * 1024)
			// Insert 1KB ranges every 16 KB.
			for i, j := 0, 0; i < 16; i, j = i+1, j+16*1024 {
				tree.Insert("A", []byte{}, uint64(j), 1024, 0, 0)
			}
			// Insert 1KB ranges every 16 KB, offset by 8KB.
			for i := 15*16*1024 + 8*1024; i >= 0; i -= 16 * 1024 {
				tree.Insert("A", []byte{}, uint64(i), 1024, 0, 0)
			}
			assertTree(t, tree)
		})
		t.Run("DescendingThenAscending", func(t *testing.T) {
			tree := NewTree(8 * 1024)
			// Insert 1KB ranges every 16 KB, offset by 8KB.
			for i := 15*16*1024 + 8*1024; i >= 0; i -= 16 * 1024 {
				tree.Insert("A", []byte{}, uint64(i), 1024, 0, 0)
			}
			// Insert 1KB ranges every 16 KB.
			for i, j := 0, 0; i < 16; i, j = i+1, j+16*1024 {
				tree.Insert("A", []byte{}, uint64(j), 1024, 0, 0)
			}
			assertTree(t, tree)
		})
		t.Run("Shuffled", func(t *testing.T) {
			var entries []uint64
			for i := 15*16*1024 + 8*1024; i >= 0; i -= 16 * 1024 {
				entries = append(entries, uint64(i))
			}
			for i, j := 0, 0; i < 16; i, j = i+1, j+16*1024 {
				entries = append(entries, uint64(j))
			}
			for i := 0; i < 32; i++ {
				rand.Shuffle(len(entries), func(i, j int) {
					entries[i], entries[j] = entries[j], entries[i]
				})
				tree := NewTree(8 * 1024)
				for _, offset := range entries {
					tree.Insert("A", []byte{}, offset, 1024, 0, 0)
				}
				assertTree(t, tree)
			}
		})
	})
	t.Run("SeparateUrls", func(t *testing.T) {
		tree := NewTree(8 * 1024)
		files := []string{
			"D", "E", "C", "F",
			"0", "1", "2", "3",
			"7", "6", "5", "4",
			"B", "A", "9", "8",
		}
		for i, j := 0, 0; i < 16; i, j = i+1, j+1024 {
			tree.Insert(files[i], []byte{}, uint64(j), 1024, 0, 0)
		}
		assert.Equal(t, 16, tree.regions.Len())
		assert.Equal(t, 16, tree.t.Len())
	})
	t.Run("MergeInMiddle", func(t *testing.T) {
		tree := NewTree(8 * 1024)
		// 1KB chunk at byte 0
		tree.Insert("A", []byte{}, 0, 1024, 0, 0)
		// 1KB chunk at byte 16KB
		tree.Insert("A", []byte{}, 16384, 1024, 0, 0)
		assert.Equal(t, 2, tree.regions.Len())
		assert.Equal(t, 2, tree.t.Len())
		// 1KB chunk at byte 8KB
		tree.Insert("A", []byte{}, 8192, 1024, 0, 0)
		assert.Equal(t, 1, tree.regions.Len())
		assert.Equal(t, 3, tree.t.Len())
		tree.Insert("A", []byte{}, 4096, 1024, 0, 0)
		tree.Insert("A", []byte{}, 12228, 1024, 0, 0)
		assert.Equal(t, 1, tree.regions.Len())
		assert.Equal(t, 5, tree.t.Len())
		e, _ := tree.t.Min()
		assert.Equal(t, e.Region.MatchedBytes, uint64(5*1024))
	})
	t.Run("SimpleGet", func(t *testing.T) {
		assertTree := func(t *testing.T, tree *Tree) {
			assert.Equal(t, 5, tree.Len())
			ranges := tree.DeleteMaxRegion()
			if assert.Len(t, ranges, 1) {
				assert.Equal(t, byte(4), ranges[0].Hash[0])
			}
			assert.Equal(t, 4, tree.Len())
			ranges = tree.DeleteMaxRegion()
			if assert.Len(t, ranges, 4) {
				for i := range ranges {
					assert.Equal(t, byte(i), ranges[i].Hash[0])
				}
			}
			assert.Equal(t, 0, tree.Len())
			ranges = tree.DeleteMaxRegion()
			assert.Len(t, ranges, 0)
		}
		type entry struct {
			url    string
			id     byte
			offset uint64
			length uint32
		}
		entries := []entry{
			{"A", 0, 0, 1024},
			{"A", 1, 1024, 1024},
			{"A", 2, 2048, 1024},
			{"A", 3, 3074, 1024},
			{"B", 4, 0, 8 * 1024},
		}
		t.Run("InsertAscending", func(t *testing.T) {
			tree := NewTree(4 * 1024)
			for _, e := range entries {
				tree.Insert(e.url, []byte{e.id}, e.offset, e.length, 0, 0)
			}
			assertTree(t, tree)
		})
		t.Run("InsertDescending", func(t *testing.T) {
			tree := NewTree(4 * 1024)
			for i := len(entries) - 1; i >= 0; i-- {
				e := entries[i]
				tree.Insert(e.url, []byte{e.id}, e.offset, e.length, 0, 0)
			}
			assertTree(t, tree)
		})
		t.Run("InsertRandom", func(t *testing.T) {
			shuffled := make([]entry, len(entries))
			copy(shuffled, entries)
			for i := 0; i < 64; i++ {
				rand.Shuffle(len(shuffled), func(i, j int) {
					shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				})
				tree := NewTree(4 * 1024)
				for _, e := range entries {
					tree.Insert(e.url, []byte{e.id}, e.offset, e.length, 0, 0)
				}
				assertTree(t, tree)
			}
		})
	})
}
