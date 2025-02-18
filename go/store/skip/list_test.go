// Copyright 2021 Dolthub, Inc.
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

package skip

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

var randSrc = rand.New(rand.NewSource(0))

func TestSkipList(t *testing.T) {
	t.Run("test skip list", func(t *testing.T) {
		vals := [][]byte{
			b("a"), b("b"), b("c"), b("d"), b("e"),
			b("f"), b("g"), b("h"), b("i"), b("j"),
			b("k"), b("l"), b("m"), b("n"), b("o"),
		}
		testSkipList(t, bytes.Compare, vals...)
	})

	t.Run("test skip list of random bytes", func(t *testing.T) {
		vals := randomVals((randSrc.Int63() % 10_000) + 100)
		testSkipList(t, bytes.Compare, vals...)
	})
	t.Run("test with custom compare function", func(t *testing.T) {
		compare := func(left, right []byte) int {
			l := int64(binary.LittleEndian.Uint64(left))
			r := int64(binary.LittleEndian.Uint64(right))
			return int(l - r)
		}
		vals := randomInts((randSrc.Int63() % 10_000) + 100)
		testSkipList(t, compare, vals...)
	})
}

func TestSkipListCheckpoints(t *testing.T) {
	t.Run("test skip list", func(t *testing.T) {
		vals := [][]byte{
			b("a"), b("b"), b("c"), b("d"), b("e"),
			b("f"), b("g"), b("h"), b("i"), b("j"),
			b("k"), b("l"), b("m"), b("n"), b("o"),
		}
		testSkipListCheckpoints(t, bytes.Compare, vals...)
	})

	t.Run("test skip list of random bytes", func(t *testing.T) {
		vals := randomVals((randSrc.Int63() % 10_000) + 100)
		testSkipListCheckpoints(t, bytes.Compare, vals...)
	})
	t.Run("test with custom compare function", func(t *testing.T) {
		compare := func(left, right []byte) int {
			l := int64(binary.LittleEndian.Uint64(left))
			r := int64(binary.LittleEndian.Uint64(right))
			return int(l - r)
		}
		vals := randomInts((randSrc.Int63() % 10_000) + 100)
		testSkipListCheckpoints(t, compare, vals...)
	})
}

func TestMemoryFootprint(t *testing.T) {
	var sz int
	sz = int(unsafe.Sizeof(skipNode{}))
	assert.Equal(t, 104, sz)
	sz = int(unsafe.Sizeof(tower{}))
	assert.Equal(t, 40, sz)
}

func testSkipList(t *testing.T, compare KeyOrder, vals ...[]byte) {
	randSrc.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	// |list| is shared between each test
	list := NewSkipList(compare)

	t.Run("test puts", func(t *testing.T) {
		// |list| is populated
		for _, v := range vals {
			list.Put(ctx, v, v)
		}
		testSkipListPuts(t, list, vals...)
	})
	t.Run("test gets", func(t *testing.T) {
		testSkipListGets(t, list, vals...)
	})
	t.Run("test updates", func(t *testing.T) {
		// |list| is mutated
		testSkipListUpdates(t, list, vals...)
	})
	t.Run("test iter forward", func(t *testing.T) {
		testSkipListIterForward(t, list, vals...)
	})
	t.Run("test iter backward", func(t *testing.T) {
		testSkipListIterBackward(t, list, vals...)
	})
	t.Run("test truncate", func(t *testing.T) {
		// |list| is truncated
		testSkipListTruncate(t, list, vals...)
	})
}

func testSkipListPuts(t *testing.T, list *List, vals ...[]byte) {
	assert.Equal(t, len(vals), list.Count())
}

func testSkipListGets(t *testing.T, list *List, vals ...[]byte) {
	// get in different keyOrder
	randSrc.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	for _, exp := range vals {
		act, ok := list.Get(ctx, exp)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}

	// test absent key
	act, ok := list.Get(ctx, b("12345678"))
	assert.False(t, ok)
	assert.Nil(t, act)
}

func testSkipListUpdates(t *testing.T, list *List, vals ...[]byte) {
	v2 := []byte("789")
	for _, v := range vals {
		list.Put(ctx, v, v2)
	}
	assert.Equal(t, len(vals), list.Count())

	randSrc.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})
	for _, exp := range vals {
		act, ok := list.Get(ctx, exp)
		assert.True(t, ok)
		assert.Equal(t, v2, act)
	}

	// introspect list to assert copy-on-update behavior
	assert.Equal(t, 1+len(vals)*2, len(list.nodes))
}

func testSkipListIterForward(t *testing.T, list *List, vals ...[]byte) {
	// put |vals| back in keyOrder
	sort.Slice(vals, func(i, j int) bool {
		return list.compareKeys(ctx, vals[i], vals[j]) < 0
	})

	idx := 0
	iterAll(list, func(key, val []byte) {
		assert.Equal(t, key, key)
		assert.Equal(t, vals[idx], key)
		idx++
	})
	assert.Equal(t, len(vals), idx)

	// test iter at
	for k := 0; k < 10; k++ {
		idx = randSrc.Int() % len(vals)
		key := vals[idx]
		act := validateIterForwardFrom(t, list, key)
		exp := len(vals) - idx
		assert.Equal(t, exp, act)
	}

	act := validateIterForwardFrom(t, list, vals[0])
	assert.Equal(t, len(vals), act)
	act = validateIterForwardFrom(t, list, vals[len(vals)-1])
	assert.Equal(t, 1, act)
}

func testSkipListIterBackward(t *testing.T, list *List, vals ...[]byte) {
	// put |vals| back in keyOrder
	sort.Slice(vals, func(i, j int) bool {
		return list.compareKeys(ctx, vals[i], vals[j]) < 0
	})

	// test iter at
	for k := 0; k < 10; k++ {
		idx := randSrc.Int() % len(vals)
		key := vals[idx]
		act := validateIterBackwardFrom(t, list, key)
		assert.Equal(t, idx+1, act)
	}

	act := validateIterBackwardFrom(t, list, vals[0])
	assert.Equal(t, 1, act)
	act = validateIterBackwardFrom(t, list, vals[len(vals)-1])
	assert.Equal(t, len(vals), act)
}

func testSkipListTruncate(t *testing.T, list *List, vals ...[]byte) {
	assert.Equal(t, list.Count(), len(vals))

	list.Truncate()
	assert.Equal(t, list.Count(), 0)

	for i := range vals {
		assert.False(t, list.Has(ctx, vals[i]))
	}
	for i := range vals {
		v, ok := list.Get(ctx, vals[i])
		assert.False(t, ok)
		assert.Nil(t, v)
	}

	validateIter := func(iter *ListIter) {
		k, v := iter.Current()
		assert.Nil(t, k)
		assert.Nil(t, v)
		iter.Advance()
		assert.Nil(t, k)
		assert.Nil(t, v)
		iter.Retreat()
		iter.Retreat()
		assert.Nil(t, k)
		assert.Nil(t, v)
	}

	validateIter(list.IterAtStart())
	validateIter(list.IterAtEnd())
	validateIter(list.GetIterAt(ctx, vals[0]))
}

func validateIterForwardFrom(t *testing.T, l *List, key []byte) (count int) {
	iter := l.GetIterAt(ctx, key)
	k, _ := iter.Current()
	for k != nil {
		count++
		iter.Advance()
		prev := k
		k, _ = iter.Current()
		assert.True(t, l.compareKeys(ctx, prev, k) < 0)
	}
	return
}

func validateIterBackwardFrom(t *testing.T, l *List, key []byte) (count int) {
	iter := l.GetIterAt(ctx, key)
	k, _ := iter.Current()
	for k != nil {
		count++
		iter.Retreat()
		prev := k
		k, _ = iter.Current()

		if k != nil {
			assert.True(t, l.compareKeys(ctx, prev, k) > 0)
		}
	}
	return
}

func randomVals(cnt int64) (vals [][]byte) {
	vals = make([][]byte, cnt)
	for i := range vals {
		bb := make([]byte, (randSrc.Int63()%91)+10)
		randSrc.Read(bb)
		vals[i] = bb
	}
	return
}

func randomInts(cnt int64) (vals [][]byte) {
	vals = make([][]byte, cnt)
	for i := range vals {
		vals[i] = make([]byte, 8)
		v := uint64(randSrc.Int63())
		binary.LittleEndian.PutUint64(vals[i], v)
	}
	return
}

func b(s string) []byte {
	return []byte(s)
}

func iterAll(l *List, cb func([]byte, []byte)) {
	iter := l.IterAtStart()
	key, val := iter.Current()
	for key != nil {
		cb(key, val)
		iter.Advance()
		key, val = iter.Current()
	}
}

func iterAllBackwards(l *List, cb func([]byte, []byte)) {
	iter := l.IterAtEnd()
	key, val := iter.Current()
	for key != nil {
		cb(key, val)
		iter.Retreat()
		key, val = iter.Current()
	}
}

func testSkipListCheckpoints(t *testing.T, compare KeyOrder, data ...[]byte) {
	randSrc.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	k := len(data) / 3

	init := data[:k*2]
	static := data[:k]
	updates := data[k : k*2]
	inserts := data[k*2:]

	list := NewSkipList(compare)

	// test empty revert
	list.Revert(ctx)

	for _, v := range init {
		list.Put(ctx, v, v)
	}
	for _, v := range init {
		act, ok := list.Get(ctx, v)
		assert.True(t, ok)
		assert.Equal(t, v, act)
	}
	for _, v := range inserts {
		assert.False(t, list.Has(ctx, v))
	}

	list.Checkpoint()

	up := []byte("update")
	for _, v := range updates {
		list.Put(ctx, v, up)
	}

	for _, v := range inserts {
		list.Put(ctx, v, v)
	}

	for _, v := range static {
		act, ok := list.Get(ctx, v)
		assert.True(t, ok)
		assert.Equal(t, v, act)
	}
	for _, v := range inserts {
		act, ok := list.Get(ctx, v)
		assert.True(t, ok)
		assert.Equal(t, v, act)
	}
	for _, v := range updates {
		act, ok := list.Get(ctx, v)
		assert.True(t, ok)
		assert.Equal(t, up, act)
	}

	list.Revert(ctx)

	for _, v := range init {
		act, ok := list.Get(ctx, v)
		assert.True(t, ok)
		assert.Equal(t, v, act)
	}
	for _, v := range inserts {
		assert.False(t, list.Has(ctx, v))
	}
}
