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

	"github.com/stretchr/testify/assert"
)

//var src = rand.New(rand.NewSource(time.Now().Unix()))
var src = rand.New(rand.NewSource(0))

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
		vals := randomVals((src.Int63() % 10_000) + 100)
		testSkipList(t, bytes.Compare, vals...)
	})
	t.Run("test with custom compare function", func(t *testing.T) {
		compare := func(left, right []byte) int {
			l := int64(binary.LittleEndian.Uint64(left))
			r := int64(binary.LittleEndian.Uint64(right))
			return int(l - r)
		}
		vals := randomInts((src.Int63() % 10_000) + 100)
		testSkipList(t, compare, vals...)
	})

}

func testSkipList(t *testing.T, compare ValueCmp, vals ...[]byte) {
	src.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})
	list := NewSkipList(compare)
	for _, v := range vals {
		list.Put(v, v)
	}

	t.Run("test puts", func(t *testing.T) {
		testSkipListPuts(t, list, vals...)
	})
	t.Run("test gets", func(t *testing.T) {
		testSkipListGets(t, list, vals...)
	})
	t.Run("test updates", func(t *testing.T) {
		testSkipListUpdates(t, list, vals...)
	})
	t.Run("test iter forward", func(t *testing.T) {
		testSkipListIterForward(t, list, vals...)
	})
	t.Run("test iter backward", func(t *testing.T) {
		testSkipListIterBackward(t, list, vals...)
	})
}

func testSkipListPuts(t *testing.T, list *List, vals ...[]byte) {
	assert.Equal(t, len(vals), list.Count())
}

func testSkipListGets(t *testing.T, list *List, vals ...[]byte) {
	// get in different order
	src.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	for _, exp := range vals {
		act, ok := list.Get(exp)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}

	// test absent key
	act, ok := list.Get(b("12345678"))
	assert.False(t, ok)
	assert.Nil(t, act)
}

func testSkipListUpdates(t *testing.T, list *List, vals ...[]byte) {
	v2 := []byte("789")
	for _, v := range vals {
		list.Put(v, v2)
	}
	assert.Equal(t, len(vals), list.Count())

	src.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})
	for _, exp := range vals {
		act, ok := list.Get(exp)
		assert.True(t, ok)
		assert.Equal(t, v2, act)
	}
}

func testSkipListIterForward(t *testing.T, list *List, vals ...[]byte) {
	// put |vals| back in order
	sort.Slice(vals, func(i, j int) bool {
		return list.compareKeys(vals[i], vals[j]) < 0
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
		idx = src.Int() % len(vals)
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
	// put |vals| back in order
	sort.Slice(vals, func(i, j int) bool {
		return list.compareKeys(vals[i], vals[j]) < 0
	})

	// test iter at
	for k := 0; k < 10; k++ {
		idx := src.Int() % len(vals)
		key := vals[idx]
		act := validateIterBackwardFrom(t, list, key)
		assert.Equal(t, idx+1, act)
	}

	act := validateIterBackwardFrom(t, list, vals[0])
	assert.Equal(t, 1, act)
	act = validateIterBackwardFrom(t, list, vals[len(vals)-1])
	assert.Equal(t, len(vals), act)
}

func validateIterForwardFrom(t *testing.T, l *List, key []byte) (count int) {
	iter := l.GetIterAt(key)
	k, _ := iter.Current()
	for k != nil {
		count++
		iter.Advance()
		prev := k
		k, _ = iter.Current()
		assert.True(t, l.compareKeys(prev, k) < 0)
	}
	return
}

func validateIterBackwardFrom(t *testing.T, l *List, key []byte) (count int) {
	iter := l.GetIterAt(key)
	k, _ := iter.Current()
	for k != nil {
		count++
		iter.Retreat()
		prev := k
		k, _ = iter.Current()

		if k != nil {
			assert.True(t, l.compareKeys(prev, k) > 0)
		}
	}
	return
}

func randomVals(cnt int64) (vals [][]byte) {
	vals = make([][]byte, cnt)
	for i := range vals {
		bb := make([]byte, (src.Int63()%91)+10)
		src.Read(bb)
		vals[i] = bb
	}
	return
}

func randomInts(cnt int64) (vals [][]byte) {
	vals = make([][]byte, cnt)
	for i := range vals {
		vals[i] = make([]byte, 8)
		v := uint64(src.Int63())
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
