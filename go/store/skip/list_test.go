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
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSkipList(t *testing.T) {
	t.Run("test skip list", func(t *testing.T) {
		vals := [][]byte{
			b("a"), b("b"), b("c"), b("d"), b("e"),
			b("f"), b("g"), b("h"), b("i"), b("j"),
			b("k"), b("l"), b("m"), b("n"), b("o"),
		}
		for i := 0; i < 10; i++ {
			testSkipList(t, vals...)
		}
	})
	t.Run("test random skip list", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			vals := randomVals((rand.Int63() % 10_000) + 100)
			testSkipList(t, vals...)
		}
	})
}

func testSkipList(t *testing.T, vals ...[]byte) {
	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})
	testSkipListPuts(t, vals...)
	testSkipListGets(t, vals...)
	testSkipListUpdates(t, vals...)
	testSkipListIter(t, vals...)
}

func testSkipListPuts(t *testing.T, vals ...[]byte) {
	list := NewSkipList(bytes.Compare)
	for _, v := range vals {
		list.Put(v, v)
	}
	assert.Equal(t, len(vals), list.Count())
}

func testSkipListGets(t *testing.T, vals ...[]byte) {
	list := NewSkipList(bytes.Compare)
	for _, v := range vals {
		list.Put(v, v)
	}

	// get in different order
	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	for _, exp := range vals {
		act, ok := list.Get(exp)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}

	// test absent key
	act, ok := list.Get(b("123"))
	assert.False(t, ok)
	assert.Nil(t, act)
}

func testSkipListUpdates(t *testing.T, vals ...[]byte) {
	list := NewSkipList(bytes.Compare)
	for _, v := range vals {
		list.Put(v, v)
	}

	v2 := []byte("789")
	for _, v := range vals {
		list.Put(v, v2)
	}
	assert.Equal(t, len(vals), list.Count())

	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})
	for _, exp := range vals {
		act, ok := list.Get(exp)
		assert.True(t, ok)
		assert.Equal(t, v2, act)
	}
}

func testSkipListIter(t *testing.T, vals ...[]byte) {
	list := NewSkipList(bytes.Compare)
	for _, v := range vals {
		list.Put(v, v)
	}

	// put |vals| back in order
	sort.Slice(vals, func(i, j int) bool {
		return bytes.Compare(vals[i], vals[j]) == -1
	})

	idx := 0
	IterAll(list, func(key, val []byte) {
		assert.Equal(t, key, key)
		assert.Equal(t, vals[idx], key)
		idx++
	})
	assert.Equal(t, len(vals), idx)

	// test iter at
	for k := 0; k < 10; k++ {
		idx = rand.Int() % len(vals)
		key := vals[idx]
		act := countFrom(key, list)
		exp := len(vals) - idx
		assert.Equal(t, exp, act)
	}
}

func randomVals(cnt int64) (vals [][]byte) {
	vals = make([][]byte, cnt)
	for i := range vals {
		bb := make([]byte, (rand.Int63()%91)+10)
		rand.Read(bb)
		vals[i] = bb
	}
	return
}

func b(s string) []byte {
	return []byte(s)
}

func countFrom(key []byte, l *List) (count int) {
	iter := l.IterAt(key)
	k, _ := iter.Next()
	for k != nil {
		count++
		k, _ = iter.Next()
	}
	return
}
