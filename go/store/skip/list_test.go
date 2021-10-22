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
}

func testSkipList(t *testing.T, vals ...[]byte) {
	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	list := NewSkipList(strCmp)
	for _, v := range vals {
		list.Put(v, v)
	}

	assert.Equal(t, len(vals), list.Count())

	for _, exp := range vals {
		act, ok := list.Get(exp)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}

	act, ok := list.Get(b("123"))
	assert.False(t, ok)
	assert.Nil(t, act)

	sort.Slice(vals, func(i, j int) bool {
		return strCmp(vals[i], vals[j]) == -1
	})

	i := 0
	list.Iter(func(key, val []byte) {
		assert.Equal(t, key, key)
		assert.Equal(t, vals[i], key)
		i++
	})
}

func b(s string) []byte {
	return []byte(s)
}

func strCmp(left, right []byte) int {
	l, r := string(left), string(right)
	if l < r {
		return -1
	}
	if l > r {
		return 1
	}
	return 0
}
