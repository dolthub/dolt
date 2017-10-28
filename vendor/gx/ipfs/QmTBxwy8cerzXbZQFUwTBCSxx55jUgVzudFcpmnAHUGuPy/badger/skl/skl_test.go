/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package skl

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"gx/ipfs/QmTBxwy8cerzXbZQFUwTBCSxx55jUgVzudFcpmnAHUGuPy/badger/y"
)

const arenaSize = 1 << 20

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// length iterates over skiplist to give exact size.
func length(s *Skiplist) int {
	x := s.getNext(s.head, 0)
	count := 0
	for x != nil {
		count++
		x = s.getNext(x, 0)
	}
	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := NewSkiplist(arenaSize)

	v := l.Get(key)
	require.True(t, v.Value == nil) // Cannot use require.Nil for unsafe.Pointer nil.

	for _, less := range []bool{true, false} {
		for _, allowEqual := range []bool{true, false} {
			n, found := l.findNear(key, less, allowEqual)
			require.Nil(t, n)
			require.False(t, found)
		}
	}

	it := l.NewIterator()
	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	it.Seek(key)
	require.False(t, it.Valid())

	l.DecrRef()
	require.True(t, l.valid()) // Check the reference counting.

	it.Close()
	require.False(t, l.valid()) // Check the reference counting.
}

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := NewSkiplist(arenaSize)
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	l.Put([]byte("key1"), y.MakeValueStruct(val1, 55, 0, 60000))
	l.Put([]byte("key3"), y.MakeValueStruct(val3, 56, 0, 60001))
	l.Put([]byte("key2"), y.MakeValueStruct(val2, 57, 0, 60002))

	v := l.Get([]byte("key"))
	require.True(t, v.Value == nil)

	v = l.Get([]byte("key1"))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00042", string(v.Value))
	require.EqualValues(t, 55, v.Meta)
	require.EqualValues(t, 60000, v.CASCounter)

	v = l.Get([]byte("key2"))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00052", string(v.Value))
	require.EqualValues(t, 57, v.Meta)
	require.EqualValues(t, 60002, v.CASCounter)

	v = l.Get([]byte("key3"))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00062", string(v.Value))
	require.EqualValues(t, 56, v.Meta)
	require.EqualValues(t, 60001, v.CASCounter)

	l.Put([]byte("key2"), y.MakeValueStruct(val4, 12, 0, 50000))
	v = l.Get([]byte("key2"))
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00072", string(v.Value))
	require.EqualValues(t, 12, v.Meta)
	require.EqualValues(t, 50000, v.CASCounter)
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkiplist(arenaSize)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put([]byte(fmt.Sprintf("%05d", i)),
				y.MakeValueStruct(newValue(i), 0, 0, uint64(i)))
		}(i)
	}
	wg.Wait()
	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get([]byte(fmt.Sprintf("%05d", i)))
			require.True(t, v.Value != nil)
			require.EqualValues(t, newValue(i), v.Value)
			require.EqualValues(t, i, v.CASCounter)
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, n, length(l))
}

// TestOneKey will read while writing to one single key.
func TestOneKey(t *testing.T) {
	const n = 100
	key := []byte("thekey")
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(key, y.MakeValueStruct(newValue(i), 0, 0, uint64(i)))
		}(i)
	}
	// We expect that at least some write made it such that some read returns a value.
	var sawValue int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := l.Get(key)
			if p.Value == nil {
				return
			}
			atomic.StoreInt32(&sawValue, 1)
			v, err := strconv.Atoi(string(p.Value))
			require.NoError(t, err)
			require.True(t, 0 <= v && v < n)
			require.EqualValues(t, v, p.CASCounter)
		}()
	}
	wg.Wait()
	require.True(t, sawValue > 0)
	require.EqualValues(t, 1, length(l))
}

func TestFindNear(t *testing.T) {
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put([]byte(key), y.MakeValueStruct(newValue(i), 0, 0, uint64(i)))
	}

	n, eq := l.findNear([]byte("00001"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, true)
	require.Nil(t, n)
	require.False(t, eq)

	n, eq = l.findNear([]byte("00005"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "00015", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear([]byte("00005"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "00005", string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05555"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear([]byte("05555"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05545", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05558"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05565", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "05555", string(n.key(l.arena)))
	require.False(t, eq)

	n, eq = l.findNear([]byte("09995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key(l.arena)))
	require.True(t, eq)
	n, eq = l.findNear([]byte("09995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "09985", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key(l.arena)))
	require.True(t, eq)

	n, eq = l.findNear([]byte("59995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), false, true)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key(l.arena)))
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, "09995", string(n.key(l.arena)))
	require.False(t, eq)
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	it := l.NewIterator()
	defer it.Close()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := n - 1; i >= 0; i-- {
		l.Put([]byte(fmt.Sprintf("%05d", i)),
			y.MakeValueStruct(newValue(i), 0, 0, uint64(i)))
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	it := l.NewIterator()
	defer it.Close()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := 0; i < n; i++ {
		l.Put([]byte(fmt.Sprintf("%05d", i)),
			y.MakeValueStruct(newValue(i), 0, 0, uint64(i)))
	}
	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		require.EqualValues(t, i, v.CASCounter)
		it.Prev()
	}
	require.False(t, it.Valid())
}

// TestIteratorSeek tests Seek and SeekForPrev.
func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()

	it := l.NewIterator()
	defer it.Close()

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Put([]byte(fmt.Sprintf("%05d", i*10+1000)), y.MakeValueStruct(newValue(v), 0, 0, 555))
	}
	it.Seek([]byte(""))
	require.True(t, it.Valid())
	v := it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek([]byte("01000"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek([]byte("01005"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek([]byte("01010"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek([]byte("99999"))
	require.False(t, it.Valid())

	// Try SeekForPrev.
	it.SeekForPrev([]byte(""))
	require.False(t, it.Valid())

	it.SeekForPrev([]byte("01000"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev([]byte("01005"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev([]byte("01010"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.SeekForPrev([]byte("99999"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01990", v.Value)
}

func randomKey(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewSkiplist(int64((b.N + 1) * MaxNodeSize))
			defer l.DecrRef()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						v := l.Get(randomKey(rng))
						if v.Value != nil {
							count++
						}
					} else {
						l.Put(randomKey(rng), y.MakeValueStruct(value, 0, 0, 0))
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rand.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey(rng))]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey(rng))] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}
