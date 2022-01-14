// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

const testMapSize = 8000

type genValueFn func(i int) (Value, error)

type testMap struct {
	entries     mapEntrySlice
	knownBadKey Value
}

func (tm testMap) SetValue(i int, v Value) testMap {
	entries := make([]mapEntry, 0, len(tm.entries.entries))
	entries = append(entries, tm.entries.entries...)
	entries[i].value = v
	return testMap{mapEntrySlice{entries, tm.entries.nbf}, tm.knownBadKey}
}

func (tm testMap) Remove(from, to int) testMap {
	entries := make([]mapEntry, 0, len(tm.entries.entries)-(to-from))
	entries = append(entries, tm.entries.entries[:from]...)
	entries = append(entries, tm.entries.entries[to:]...)
	return testMap{mapEntrySlice{entries, tm.entries.nbf}, tm.knownBadKey}
}

func (tm testMap) MaybeGet(key Value) (v Value, ok bool) {
	for _, entry := range tm.entries.entries {
		if entry.key.Equals(key) {
			return entry.value, true
		}
	}
	return nil, false
}

func (tm testMap) Diff(last testMap) (added []Value, removed []Value, modified []Value) {
	// Note: this could be use tm.toMap/last.toMap and then tmMap.Diff(lastMap) but the
	// purpose of this method is to be redundant.
	if len(tm.entries.entries) == 0 && len(last.entries.entries) == 0 {
		return // nothing changed
	}
	if len(tm.entries.entries) == 0 {
		// everything removed
		for _, entry := range last.entries.entries {
			removed = append(removed, entry.key)
		}
		return
	}
	if len(last.entries.entries) == 0 {
		// everything added
		for _, entry := range tm.entries.entries {
			added = append(added, entry.key)
		}
		return
	}

	for _, entry := range tm.entries.entries {
		otherValue, exists := last.MaybeGet(entry.key)
		if !exists {
			added = append(added, entry.key)
		} else if !entry.value.Equals(otherValue) {
			modified = append(modified, entry.key)
		}
	}
	for _, entry := range last.entries.entries {
		_, exists := tm.MaybeGet(entry.key)
		if !exists {
			removed = append(removed, entry.key)
		}
	}
	return
}

func (tm testMap) toMap(vrw ValueReadWriter) Map {
	keyvals := []Value{}
	for _, entry := range tm.entries.entries {
		keyvals = append(keyvals, entry.key, entry.value)
	}
	return mustMap(NewMap(context.Background(), vrw, keyvals...))
}

func toValuable(vs ValueSlice) []Valuable {
	vb := make([]Valuable, len(vs))
	for i, v := range vs {
		vb[i] = v
	}
	return vb
}

func (tm testMap) Flatten(from, to int) []Value {
	flat := make([]Value, 0, len(tm.entries.entries)*2)
	for _, entry := range tm.entries.entries[from:to] {
		flat = append(flat, entry.key)
		flat = append(flat, entry.value)
	}
	return flat
}

func (tm testMap) FlattenAll() []Value {
	return tm.Flatten(0, len(tm.entries.entries))
}

func newSortedTestMap(length int, gen genValueFn) testMap {
	keys := make(ValueSlice, 0, length)
	for i := 0; i < length; i++ {
		keys = append(keys, mustValue(gen(i)))
	}

	err := SortWithErroringLess(ValueSort{keys, Format_7_18})
	d.PanicIfError(err)

	entries := make([]mapEntry, 0, len(keys))
	for i, k := range keys {
		entries = append(entries, mapEntry{k, Float(i * 2)})
	}

	return testMap{mapEntrySlice{entries, Format_7_18}, Float(length + 2)}
}

func newTestMapFromMap(m Map) testMap {
	entries := make([]mapEntry, 0, m.Len())
	err := m.IterAll(context.Background(), func(key, value Value) error {
		entries = append(entries, mapEntry{key, value})
		return nil
	})

	d.PanicIfError(err)

	return testMap{mapEntrySlice{entries, Format_7_18}, Float(-0)}
}

func newRandomTestMap(length int, gen genValueFn) testMap {
	s := rand.NewSource(4242)
	used := map[int]bool{}

	mask := int(0xffffff)
	entries := make([]mapEntry, 0, length)
	for len(entries) < length {
		v := int(s.Int63()) & mask
		if _, ok := used[v]; !ok {
			entry := mapEntry{mustValue(gen(v)), mustValue(gen(v * 2))}
			entries = append(entries, entry)
			used[v] = true
		}
	}

	return testMap{mapEntrySlice{entries, Format_7_18}, mustValue(gen(mask + 1))}
}

func validateMap(t *testing.T, vrw ValueReadWriter, m Map, entries mapEntrySlice) {
	tm := testMap{entries: entries}
	assert.True(t, m.Equals(tm.toMap(vrw)))

	out := mapEntrySlice{}
	err := m.IterAll(context.Background(), func(k Value, v Value) error {
		out.entries = append(out.entries, mapEntry{k, v})
		return nil
	})

	require.NoError(t, err)
	assert.True(t, out.Equals(entries))
}

type mapTestSuite struct {
	collectionTestSuite
	elems testMap
}

func newMapTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int, gen genValueFn) *mapTestSuite {
	vrw := newTestValueStore()

	length := 1 << size
	keyType, err := TypeOf(mustValue(gen(0)))
	d.PanicIfError(err)
	elems := newSortedTestMap(length, gen)
	tr, err := MakeMapType(keyType, PrimitiveTypeMap[FloatKind])
	d.PanicIfError(err)
	tmap, err := NewMap(context.Background(), vrw, elems.FlattenAll()...)
	d.PanicIfError(err)
	return &mapTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    tmap,
			expectType:             tr,
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				if v2.Len() != uint64(elems.entries.Len()) {
					fmt.Println("lengths not equal:", v2.Len(), elems.entries.Len())
					return false
				}
				l2 := v2.(Map)
				idx := uint64(0)
				err := l2.Iter(context.Background(), func(key, value Value) (stop bool, err error) {
					entry := elems.entries.entries[idx]
					if !key.Equals(entry.key) {
						v1, err := EncodedValue(context.Background(), key)

						if err != nil {
							return false, err
						}

						h1, err := entry.key.Hash(Format_7_18)

						if err != nil {
							return false, err
						}

						v2, err := EncodedValue(context.Background(), entry.key)

						if err != nil {
							return false, err
						}

						h2, err := entry.key.Hash(Format_7_18)

						if err != nil {
							return false, err
						}

						fmt.Printf("%d: %s (%s)\n!=\n%s (%s)\n", idx, v1, h1, v2, h2)
						stop = true
					}
					if !value.Equals(entry.value) {
						v1, err := EncodedValue(context.Background(), value)

						if err != nil {
							return false, err
						}

						h1, err := value.Hash(Format_7_18)

						if err != nil {
							return false, err
						}

						v2, err := EncodedValue(context.Background(), entry.value)

						if err != nil {
							return false, err
						}

						h2, err := entry.value.Hash(Format_7_18)

						if err != nil {
							return false, err
						}

						fmt.Printf("%s (%s) !=\n%s (%s)\n", v1, h1, v2, h2)
						stop = true
					}
					idx++
					return stop, nil
				})
				d.PanicIfError(err)
				return idx == v2.Len()
			},
			prependOne: func() (Collection, error) {
				dup := make([]mapEntry, length+1)
				dup[0] = mapEntry{Float(-1), Float(-2)}
				copy(dup[1:], elems.entries.entries)
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(context.Background(), vrw, flat...)
			},
			appendOne: func() (Collection, error) {
				dup := make([]mapEntry, length+1)
				copy(dup, elems.entries.entries)
				dup[len(dup)-1] = mapEntry{Float(length*2 + 1), Float((length*2 + 1) * 2)}
				flat := []Value{}
				for _, entry := range dup {
					flat = append(flat, entry.key, entry.value)
				}
				return NewMap(context.Background(), vrw, flat...)
			},
		},
		elems: elems,
	}
}

func (suite *mapTestSuite) createStreamingMap(vs *ValueStore) Map {
	kvChan := make(chan Value)
	streamingMap := NewStreamingMap(context.Background(), vs, kvChan)
	for _, entry := range suite.elems.entries.entries {
		kvChan <- entry.key
		kvChan <- entry.value
	}
	close(kvChan)
	m, err := streamingMap.Wait()
	suite.NoError(err)
	return m
}

func (suite *mapTestSuite) TestStreamingMap() {
	vs := newTestValueStore()
	defer vs.Close()
	m := suite.createStreamingMap(vs)
	suite.True(suite.validate(m), "map not valid")
}

func (suite *mapTestSuite) TestStreamingMapOrder() {
	vs := newTestValueStore()
	defer vs.Close()

	entries := mapEntrySlice{make([]mapEntry, len(suite.elems.entries.entries)), Format_7_18}
	copy(entries.entries, suite.elems.entries.entries)
	entries.entries[0], entries.entries[1] = entries.entries[1], entries.entries[0]

	kvChan := make(chan Value, len(entries.entries)*2)
	for _, e := range entries.entries {
		kvChan <- e.key
		kvChan <- e.value
	}
	close(kvChan)

	sm := NewStreamingMap(context.Background(), vs, kvChan)
	_, err := sm.Wait()

	suite.Assert().EqualError(err, ErrKeysNotOrdered.Error())
}

func (suite *mapTestSuite) TestStreamingMap2() {
	wg := sync.WaitGroup{}
	vs := newTestValueStore()
	defer vs.Close()

	wg.Add(2)
	var m1, m2 Map
	go func() {
		m1 = suite.createStreamingMap(vs)
		wg.Done()
	}()
	go func() {
		m2 = suite.createStreamingMap(vs)
		wg.Done()
	}()
	wg.Wait()
	suite.True(suite.validate(m1), "map 'm1' not valid")
	suite.True(suite.validate(m2), "map 'm2' not valid")
}

func TestMapSuite4K(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, 4, 2, 2, newNumber))
}

func TestMapSuite4KStructs(t *testing.T) {
	suite.Run(t, newMapTestSuite(12, 11, 2, 2, newNumberStruct))
}

func newNumber(i int) (Value, error) {
	return Float(i), nil
}

func newNumberStruct(i int) (Value, error) {
	return NewStruct(Format_7_18, "", StructData{"n": Float(i)})
}

func getTestNativeOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefValueOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, newNumber)
}

func getTestRefToNativeOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) (Value, error) {
		return vrw.WriteValue(context.Background(), Float(i))
	})
}

func getTestRefToValueOrderMap(scale int, vrw ValueReadWriter) testMap {
	return newRandomTestMap(64*scale, func(i int) (Value, error) {
		s, err := NewSet(context.Background(), vrw, Float(i))

		if err != nil {
			return nil, err
		}

		return vrw.WriteValue(context.Background(), s)
	})
}

func accumulateMapDiffChanges(m1, m2 Map) (added []Value, removed []Value, modified []Value, err error) {
	changes := make(chan ValueChanged)
	go func() {
		defer close(changes)
		err = m1.Diff(context.Background(), m2, changes)
	}()
	for change := range changes {
		if change.ChangeType == DiffChangeAdded {
			added = append(added, change.Key)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.Key)
		} else {
			modified = append(modified, change.Key)
		}
	}
	return added, removed, modified, err
}

func diffMapTest(assert *assert.Assertions, m1 Map, m2 Map, numAddsExpected int, numRemovesExpected int, numModifiedExpected int) (added []Value, removed []Value, modified []Value) {
	var err error
	added, removed, modified, err = accumulateMapDiffChanges(m1, m2)
	assert.NoError(err)
	assert.Equal(numAddsExpected, len(added), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(removed), "num removed is not as expected")
	assert.Equal(numModifiedExpected, len(modified), "num modified is not as expected")

	tm1 := newTestMapFromMap(m1)
	tm2 := newTestMapFromMap(m2)
	tmAdded, tmRemoved, tmModified := tm1.Diff(tm2)
	assert.Equal(numAddsExpected, len(tmAdded), "num added is not as expected")
	assert.Equal(numRemovesExpected, len(tmRemoved), "num removed is not as expected")
	assert.Equal(numModifiedExpected, len(tmModified), "num modified is not as expected")

	assert.Equal(added, tmAdded, "map added != tmMap added")
	assert.Equal(removed, tmRemoved, "map removed != tmMap removed")
	assert.Equal(modified, tmModified, "map modified != tmMap modified")
	return
}

func TestMapDiff(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testMap1 := newRandomTestMap(64*2, newNumber)
	testMap2 := newRandomTestMap(64*2, newNumber)
	testMapAdded, testMapRemoved, testMapModified := testMap1.Diff(testMap2)
	map1 := testMap1.toMap(vrw)
	map2 := testMap2.toMap(vrw)

	mapDiffAdded, mapDiffRemoved, mapDiffModified, err := accumulateMapDiffChanges(map1, map2)
	require.NoError(t, err)
	assert.Equal(t, testMapAdded, mapDiffAdded, "testMap.diff != map.diff")
	assert.Equal(t, testMapRemoved, mapDiffRemoved, "testMap.diff != map.diff")
	assert.Equal(t, testMapModified, mapDiffModified, "testMap.diff != map.diff")
}

func TestMapMutationReadWriteCount(t *testing.T) {
	// This test is a sanity check that we are reading a "reasonable" number of
	// sequences while mutating maps.
	// TODO: We are currently un-reasonable.
	temp := MakeStructTemplate("Foo", []string{"Bool", "Number", "String1", "String2"})

	newLargeStruct := func(i int) (Value, error) {
		return temp.NewStruct(Format_7_18, []Value{
			Bool(i%2 == 0),
			Float(i),
			String(fmt.Sprintf("I AM A REALLY REALY REALL SUPER CALIFRAGILISTICLY CRAZY-ASSED LONGTASTIC String %d", i)),
			String(fmt.Sprintf("I am a bit shorter and also more chill: %d", i)),
		})
	}

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := newValueStoreWithCacheAndPending(cs, 0, 0)

	numEdits := 10000
	vals := make([]Value, 0, numEdits)
	m, err := NewMap(context.Background(), vs)
	require.NoError(t, err)
	me := m.Edit()
	for i := 0; i < 10000; i++ {
		s, err := newLargeStruct(i)
		require.NoError(t, err)
		vals = append(vals, s)
		me.Set(Float(i), s)
	}
	m, err = me.Map(context.Background())
	require.NoError(t, err)
	r, err := vs.WriteValue(context.Background(), m)
	require.NoError(t, err)
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)
	v, err := r.TargetValue(context.Background(), vs)
	require.NoError(t, err)
	m = v.(Map)
	require.NoError(t, err)

	every := 100

	me = m.Edit()
	for i := 0; i < 10000; i++ {
		if i%every == 0 {
			k := Float(i)
			s := vals[i].(Struct)
			n, ok, err := s.MaybeGet("Number")
			require.NoError(t, err)
			assert.True(t, ok)
			s, err = s.Set("Number", Float(float64(n.(Float))+1))
			require.NoError(t, err)
			me.Set(k, s)
		}
	}

	wrCnt := cs.Writes()
	rdCnt := cs.Reads()

	m, err = me.Map(context.Background())
	require.NoError(t, err)

	rt, err = vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)

	ref, err := NewRef(m, Format_7_18)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), ref.Height())
	assert.Equal(t, 105, cs.Reads()-rdCnt)
	assert.Equal(t, 62, cs.Writes()-wrCnt)
}

func TestMapInfiniteChunkBug(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	keyLen := chunkWindow + 1

	buff := &bytes.Buffer{}
	for i := uint32(0); i < keyLen; i++ {
		buff.WriteString("s")
	}

	prefix := buff.String()

	m, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	me := m.Edit()

	for i := 0; i < 10000; i++ {
		me.Set(String(prefix+fmt.Sprintf("%d", i)), Float(i))
	}

	_, err = me.Map(context.Background())
	require.NoError(t, err)
}

func TestNewMap(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	assert.Equal(uint64(0), m.Len())
	m, err = NewMap(context.Background(), vrw, String("foo1"), String("bar1"), String("foo2"), String("bar2"))
	require.NoError(t, err)
	assert.Equal(uint64(2), m.Len())
	foo1Str, ok, err := m.MaybeGet(context.Background(), String("foo1"))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(String("bar1").Equals(foo1Str))
	foo2Str, ok, err := m.MaybeGet(context.Background(), String("foo2"))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(String("bar2").Equals(foo2Str))
}

func TestMapUniqueKeysString(t *testing.T) {
	vrw := newTestValueStore()

	assert := assert.New(t)
	l := []Value{
		String("hello"), String("world"),
		String("foo"), String("bar"),
		String("bar"), String("foo"),
		String("hello"), String("foo"),
	}
	m, err := NewMap(context.Background(), vrw, l...)
	require.NoError(t, err)
	assert.Equal(uint64(3), m.Len())
	v, ok, err := m.MaybeGet(context.Background(), String("hello"))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(String("foo").Equals(v))
}

func TestMapUniqueKeysNumber(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	l := []Value{
		Float(4), Float(1),
		Float(0), Float(2),
		Float(1), Float(2),
		Float(3), Float(4),
		Float(1), Float(5),
	}
	m, err := NewMap(context.Background(), vrw, l...)
	require.NoError(t, err)
	assert.Equal(uint64(4), m.Len())
	v, ok, err := m.MaybeGet(context.Background(), Float(1))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(Float(5).Equals(v))
}

type toTestMapFunc func(scale int, vrw ValueReadWriter) testMap

func TestMapHas(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		ref, err := vrw.WriteValue(context.Background(), m)
		require.NoError(t, err)
		mval2, err := vrw.ReadValue(context.Background(), ref.TargetHash())
		require.NoError(t, err)
		m2 := mval2.(Map)
		for _, entry := range tm.entries.entries {
			k, v := entry.key, entry.value
			assert.True(m.Has(context.Background(), k))
			kv, ok, err := m.MaybeGet(context.Background(), k)
			require.NoError(t, err)
			assert.True(ok)
			assert.True(kv.Equals(v))
			assert.True(m2.Has(context.Background(), k))
			kv, ok, err = m2.MaybeGet(context.Background(), k)
			require.NoError(t, err)
			assert.True(ok)
			assert.True(kv.Equals(v))
		}
		diffMapTest(assert, m, m2, 0, 0, 0)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func hasAll(m Map, keys ...string) bool {
	ctx := context.Background()
	for _, k := range keys {
		has, err := m.Has(ctx, String(k))
		d.PanicIfError(err)
		if !has {
			return false
		}
	}

	return true
}

func hasNone(m Map, keys ...string) bool {
	ctx := context.Background()
	for _, k := range keys {
		has, err := m.Has(ctx, String(k))
		d.PanicIfError(err)
		if has {
			return false
		}
	}

	return true
}

func TestMapHasRemove(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	me := m.Edit()

	initial := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"}
	initialUnexpected := []string{"n", "o", "p", "q", "r", "s"}
	for _, k := range initial {
		me.Set(String(k), Int(0))
	}

	m, err = me.Map(context.Background())
	require.NoError(t, err)
	assert.True(m.Len() == uint64(len(initial)))
	assert.True(hasAll(m, initial...))
	assert.True(hasNone(m, initialUnexpected...))

	me = m.Edit()
	// add new
	me.Set(String("n"), Int(1))

	// remove
	me.Remove(String("b"))

	// remove and re-add
	me.Remove(String("c"))
	me.Set(String("c"), Int(1))

	// set then remove
	me.Set(String("d"), Int(2))
	me.Remove(String("d"))

	// add then remove
	me.Set(String("o"), Int(1))
	me.Remove(String("o"))

	// In-order insertions
	me.Set(String("p"), Int(1))
	me.Set(String("q"), Int(1))

	// Out-of-order insertions
	me.Set(String("s"), Int(1))
	me.Set(String("r"), Int(1))

	m, err = me.Map(context.Background())
	require.NoError(t, err)
	expected := []string{"a", "c", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "p", "q", "r", "s"}
	unexpected := []string{"b", "d", "o"}
	assert.True(hasAll(m, expected...))
	assert.True(hasNone(m, unexpected...))

	assert.True(m.Len() == uint64(len(expected)))
}

func TestMapRemove(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr int, toTestMap toTestMapFunc, scale int) {
		vs := newTestValueStore()
		tm := toTestMap(scale, vs)
		whole := tm.toMap(vs)
		run := func(i int) {
			expected := tm.Remove(i, i+1).toMap(vs)
			actual, err := whole.Edit().Remove(tm.entries.entries[i].key).Map(context.Background())
			require.NoError(t, err)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries.entries); i += incr {
			run(i)
		}
		run(len(tm.entries.entries) - 1)
	}

	doTest(128, getTestNativeOrderMap, 32)
	doTest(64, getTestRefValueOrderMap, 4)
	doTest(64, getTestRefToNativeOrderMap, 4)
	doTest(64, getTestRefToValueOrderMap, 4)
}

func TestMapRemoveNonexistentKey(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	tm := getTestNativeOrderMap(2, vrw)
	original := tm.toMap(vrw)
	actual, err := original.Edit().Remove(Float(-1)).Map(context.Background()) // rand.Int63 returns non-negative numbers.
	require.NoError(t, err)

	assert.Equal(original.Len(), actual.Len())
	assert.True(original.Equals(actual))
}

func TestMapFirst(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	m1, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	k, v, err := m1.First(context.Background())
	require.NoError(t, err)
	assert.Nil(k)
	assert.Nil(v)

	m1, err = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	require.NoError(t, err)
	ak, av, err := m1.First(context.Background())
	require.NoError(t, err)
	var ek, ev Value

	err = m1.Iter(context.Background(), func(k, v Value) (stop bool, err error) {
		ek, ev = k, v
		return true, nil
	})

	require.NoError(t, err)
	assert.True(ek.Equals(ak))
	assert.True(ev.Equals(av))
}

func TestMapFirst2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		err := SortWithErroringLess(tm.entries)
		require.NoError(t, err)
		actualKey, actualValue, err := m.First(context.Background())
		require.NoError(t, err)
		assert.True(tm.entries.entries[0].key.Equals(actualKey))
		assert.True(tm.entries.entries[0].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapLast(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	m1, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	k, v, err := m1.First(context.Background())
	require.NoError(t, err)
	assert.Nil(k)
	assert.Nil(v)

	m1, err = m1.Edit().Set(String("foo"), String("bar")).Set(String("hot"), String("dog")).Map(context.Background())
	require.NoError(t, err)
	ak, av, err := m1.Last(context.Background())
	require.NoError(t, err)
	var ek, ev Value

	err = m1.Iter(context.Background(), func(k, v Value) (stop bool, err error) {
		ek, ev = k, v
		return false, nil
	})

	require.NoError(t, err)
	assert.True(ek.Equals(ak))
	assert.True(ev.Equals(av))
}

func TestMapLast2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		err := SortWithErroringLess(tm.entries)
		require.NoError(t, err)
		actualKey, actualValue, err := m.Last(context.Background())
		require.NoError(t, err)
		assert.True(tm.entries.entries[len(tm.entries.entries)-1].key.Equals(actualKey))
		assert.True(tm.entries.entries[len(tm.entries.entries)-1].value.Equals(actualValue))
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapSetGet(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()
	ctx := context.Background()

	assertMapVal := func(me *MapEditor, k, expectedVal Value) *MapEditor {
		m, err := me.Map(ctx)
		require.NoError(t, err)
		mV, ok, err := m.MaybeGet(ctx, k)
		require.NoError(t, err)
		assert.True(ok == (expectedVal != nil))
		assert.True((expectedVal == nil && mV == nil) || expectedVal.Equals(mV))
		return m.Edit()
	}

	m, err := NewMap(ctx, vrw)
	require.NoError(t, err)
	me := m.Edit()
	me = assertMapVal(me, String("a"), nil)

	me.Set(String("a"), Float(42))
	me = assertMapVal(me, String("a"), Float(42))

	me.Set(String("a"), Float(43))
	me = assertMapVal(me, String("a"), Float(43))

	me.Remove(String("a"))
	me = assertMapVal(me, String("a"), nil)

	// in-order insertions
	me.Set(String("b"), Float(43))
	me.Set(String("c"), Float(44))

	me = assertMapVal(me, String("b"), Float(43))
	me = assertMapVal(me, String("c"), Float(44))

	// out-of-order insertions
	me.Set(String("z"), Float(0))
	me.Set(String("y"), Float(1))

	me = assertMapVal(me, String("z"), Float(0))
	me = assertMapVal(me, String("y"), Float(1))

	// removals
	me.Remove(String("z"))
	me.Remove(String("a"))
	me.Remove(String("y"))
	me.Remove(String("b"))
	me.Remove(String("c"))

	me = assertMapVal(me, String("a"), nil)
	me = assertMapVal(me, String("b"), nil)
	me = assertMapVal(me, String("c"), nil)
	me = assertMapVal(me, String("y"), nil)
	me = assertMapVal(me, String("z"), nil)
	me = assertMapVal(me, String("never-inserted"), nil)

	m, err = me.Map(context.Background())
	require.NoError(t, err)
	assert.True(m.Len() == 0)
}

func validateMapInsertion(t *testing.T, tm testMap) {
	vrw := newTestValueStore()
	ctx := context.Background()

	allMap, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	allMe := allMap.Edit()
	incrMap, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	incrMe := incrMap.Edit()

	for _, entry := range tm.entries.entries {
		allMe.Set(entry.key, entry.value)
		incrMe.Set(entry.key, entry.value)

		currIncrMap, err := incrMe.Map(ctx)
		require.NoError(t, err)
		incrMe = currIncrMap.Edit()
	}

	m1, err := allMe.Map(ctx)
	require.NoError(t, err)
	m2, err := incrMe.Map(ctx)
	require.NoError(t, err)

	validateMap(t, vrw, m1, tm.entries)
	validateMap(t, vrw, m2, tm.entries)
}

func TestMapValidateInsertAscending(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	validateMapInsertion(t, newSortedTestMap(300, newNumber))
}

func TestMapSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(incr, offset int, toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		expected := tm.toMap(vrw)
		run := func(from, to int) {
			actual, err := tm.Remove(from, to).toMap(vrw).Edit().SetM(toValuable(tm.Flatten(from, to))...).Map(context.Background())
			require.NoError(t, err)
			assert.Equal(expected.Len(), actual.Len())
			assert.True(expected.Equals(actual))
			diffMapTest(assert, expected, actual, 0, 0, 0)
		}
		for i := 0; i < len(tm.entries.entries)-offset; i += incr {
			run(i, i+offset)
		}
		run(len(tm.entries.entries)-offset, len(tm.entries.entries))
	}

	doTest(18, 3, getTestNativeOrderMap, 9)
	doTest(128, 1, getTestNativeOrderMap, 32)
	doTest(64, 1, getTestRefValueOrderMap, 4)
	doTest(64, 1, getTestRefToNativeOrderMap, 4)
	doTest(64, 1, getTestRefToValueOrderMap, 4)
}

func TestMapSetM(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m1, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	m2, err := m1.Edit().SetM().Map(context.Background())
	require.NoError(t, err)
	assert.True(m1.Equals(m2))
	m3, err := m2.Edit().SetM(String("foo"), String("bar"), String("hot"), String("dog")).Map(context.Background())
	require.NoError(t, err)
	assert.Equal(uint64(2), m3.Len())
	fooStr, ok, err := m3.MaybeGet(context.Background(), String("foo"))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(String("bar").Equals(fooStr))
	hotStr, ok, err := m3.MaybeGet(context.Background(), String("hot"))
	require.NoError(t, err)
	assert.True(ok)
	assert.True(String("dog").Equals(hotStr))
	m4, err := m3.Edit().SetM(String("mon"), String("key")).Map(context.Background())
	require.NoError(t, err)
	assert.Equal(uint64(2), m3.Len())
	assert.Equal(uint64(3), m4.Len())
}

func TestMapSetExistingKeyToNewValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	assert := assert.New(t)

	tm := getTestNativeOrderMap(2, vrw)
	original := tm.toMap(vrw)

	expectedWorking := tm
	actual := original
	for i, entry := range tm.entries.entries {
		newValue := Float(int64(entry.value.(Float)) + 1)
		expectedWorking = expectedWorking.SetValue(i, newValue)

		var err error
		actual, err = actual.Edit().Set(entry.key, newValue).Map(context.Background())
		require.NoError(t, err)
	}

	expected := expectedWorking.toMap(vrw)
	assert.Equal(expected.Len(), actual.Len())
	assert.True(expected.Equals(actual))
	assert.False(original.Equals(actual))
	diffMapTest(assert, expected, actual, 0, 0, 0)
}

// BUG 98
func TestMapDuplicateSet(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m1, err := NewMap(context.Background(), vrw, Bool(true), Bool(true), Float(42), Float(42), Float(42), Float(42))
	require.NoError(t, err)
	assert.Equal(uint64(2), m1.Len())
}

func TestMapMaybeGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		for _, entry := range tm.entries.entries {
			v, ok, err := m.MaybeGet(context.Background(), entry.key)
			require.NoError(t, err)
			if assert.True(ok, "%v should have been in the map!", entry.key) {
				assert.True(v.Equals(entry.value), "%v != %v", v, entry.value)
			}
		}
		_, ok, err := m.MaybeGet(context.Background(), tm.knownBadKey)
		require.NoError(t, err)
		assert.False(ok, "m should not contain %v", tm.knownBadKey)
	}

	doTest(getTestNativeOrderMap, 2)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapIter(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	m, err := NewMap(context.Background(), vrw)

	require.NoError(t, err)

	type entry struct {
		key   Value
		value Value
	}

	type resultList []entry
	results := resultList{}
	got := func(key, val Value) bool {
		for _, r := range results {
			if key.Equals(r.key) && val.Equals(r.value) {
				return true
			}
		}
		return false
	}

	stop := false
	cb := func(k, v Value) (bool, error) {
		results = append(results, entry{k, v})
		return stop, nil
	}

	err = m.Iter(context.Background(), cb)
	require.NoError(t, err)
	assert.Equal(0, len(results))

	m, err = m.Edit().Set(String("a"), Float(0)).Set(String("b"), Float(1)).Map(context.Background())
	require.NoError(t, err)
	err = m.Iter(context.Background(), cb)
	require.NoError(t, err)
	assert.Equal(2, len(results))
	assert.True(got(String("a"), Float(0)))
	assert.True(got(String("b"), Float(1)))

	results = resultList{}
	stop = true
	err = m.Iter(context.Background(), cb)
	require.NoError(t, err)
	assert.Equal(1, len(results))
	// Iteration order not guaranteed, but it has to be one of these.
	assert.True(got(String("a"), Float(0)) || got(String("b"), Float(1)))
}

func TestMapIter2(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		err := SortWithErroringLess(tm.entries)
		require.NoError(t, err)
		idx := uint64(0)
		endAt := uint64(64)

		err = m.Iter(context.Background(), func(k, v Value) (done bool, err error) {
			assert.True(tm.entries.entries[idx].key.Equals(k))
			assert.True(tm.entries.entries[idx].value.Equals(v))
			if idx == endAt {
				done = true
			}
			idx++
			return done, nil
		})

		require.NoError(t, err)
		assert.Equal(endAt, idx-1)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapAny(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	p := func(k, v Value) bool {
		return k.Equals(String("foo")) && v.Equals(String("bar"))
	}

	assert.False(mustMap(NewMap(context.Background(), vrw)).Any(context.Background(), p))
	assert.False(mustMap(NewMap(context.Background(), vrw, String("foo"), String("baz"))).Any(context.Background(), p))
	assert.True(mustMap(NewMap(context.Background(), vrw, String("foo"), String("bar"))).Any(context.Background(), p))
}

func TestMapIterAll(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}

	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	doTest := func(toTestMap toTestMapFunc, scale int) {
		vrw := newTestValueStore()
		tm := toTestMap(scale, vrw)
		m := tm.toMap(vrw)
		err := SortWithErroringLess(tm.entries)
		require.NoError(t, err)
		idx := uint64(0)

		err = m.IterAll(context.Background(), func(k, v Value) error {
			assert.True(tm.entries.entries[idx].key.Equals(k))
			assert.True(tm.entries.entries[idx].value.Equals(v))
			idx++

			return nil
		})

		require.NoError(t, err)
	}

	doTest(getTestNativeOrderMap, 16)
	doTest(getTestRefValueOrderMap, 2)
	doTest(getTestRefToNativeOrderMap, 2)
	doTest(getTestRefToValueOrderMap, 2)
}

func TestMapEquals(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	m1, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	m2 := m1
	m3, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)

	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.True(m3.Equals(m2))
	assert.True(m2.Equals(m3))
	diffMapTest(assert, m1, m2, 0, 0, 0)
	diffMapTest(assert, m1, m3, 0, 0, 0)
	diffMapTest(assert, m2, m1, 0, 0, 0)
	diffMapTest(assert, m2, m3, 0, 0, 0)
	diffMapTest(assert, m3, m1, 0, 0, 0)
	diffMapTest(assert, m3, m2, 0, 0, 0)

	m1, err = NewMap(context.Background(), vrw, String("foo"), Float(0.0), String("bar"), mustList(NewList(context.Background(), vrw)))
	require.NoError(t, err)
	m2, err = m2.Edit().Set(String("foo"), Float(0.0)).Set(String("bar"), mustList(NewList(context.Background(), vrw))).Map(context.Background())
	require.NoError(t, err)
	assert.True(m1.Equals(m2))
	assert.True(m2.Equals(m1))
	assert.False(m2.Equals(m3))
	assert.False(m3.Equals(m2))
	diffMapTest(assert, m1, m2, 0, 0, 0)
	diffMapTest(assert, m1, m3, 2, 0, 0)
	diffMapTest(assert, m2, m1, 0, 0, 0)
	diffMapTest(assert, m2, m3, 2, 0, 0)
	diffMapTest(assert, m3, m1, 0, 2, 0)
	diffMapTest(assert, m3, m2, 0, 2, 0)
}

func TestMapNotStringKeys(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()
	l := []Value{
		Bool(true), String("true"),
		Bool(false), String("false"),
		Float(1), String("Float: 1"),
		Float(0), String("Float: 0"),
		mustList(NewList(context.Background(), vrw)), String("empty list"),
		mustList(NewList(context.Background(), vrw, mustList(NewList(context.Background(), vrw)))), String("list of list"),
		mustMap(NewMap(context.Background(), vrw)), String("empty map"),
		mustMap(NewMap(context.Background(), vrw, mustMap(NewMap(context.Background(), vrw)), mustMap(NewMap(context.Background(), vrw)))), String("map of map/map"),
		mustValue(NewSet(context.Background(), vrw)), String("empty set"),
		mustValue(NewSet(context.Background(), vrw, mustValue(NewSet(context.Background(), vrw)))), String("map of set/set"),
	}
	m1, err := NewMap(context.Background(), vrw, l...)
	require.NoError(t, err)
	assert.Equal(uint64(10), m1.Len())
	for i := 0; i < len(l); i += 2 {
		v, ok, err := m1.MaybeGet(context.Background(), l[i])
		require.NoError(t, err)
		assert.True(ok)
		assert.True(v.Equals(l[i+1]))
	}
	v, ok, err := m1.MaybeGet(context.Background(), Float(42))
	require.NoError(t, err)
	assert.False(ok)
	assert.Nil(v)
}

func testMapOrder(assert *assert.Assertions, vrw ValueReadWriter, keyType, valueType *Type, tuples []Value, expectOrdering []Value) {
	m, err := NewMap(context.Background(), vrw, tuples...)
	assert.NoError(err)
	i := 0
	err = m.IterAll(context.Background(), func(key, value Value) error {
		hi, err := expectOrdering[i].Hash(Format_7_18)
		assert.NoError(err)
		kh, err := key.Hash(Format_7_18)
		assert.NoError(err)
		assert.Equal(hi.String(), kh.String())
		i++
		return nil
	})

	assert.NoError(err)
}

func TestMapOrdering(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vrw := newTestValueStore()

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[StringKind], PrimitiveTypeMap[StringKind],
		[]Value{
			String("a"), String("unused"),
			String("z"), String("unused"),
			String("b"), String("unused"),
			String("y"), String("unused"),
			String("c"), String("unused"),
			String("x"), String("unused"),
		},
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Float(0), String("unused"),
			Float(1000), String("unused"),
			Float(1), String("unused"),
			Float(100), String("unused"),
			Float(2), String("unused"),
			Float(10), String("unused"),
		},
		[]Value{
			Float(0),
			Float(1),
			Float(2),
			Float(10),
			Float(100),
			Float(1000),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[UintKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Uint(0), String("unused"),
			Uint(1000), String("unused"),
			Uint(1), String("unused"),
			Uint(100), String("unused"),
			Uint(2), String("unused"),
			Uint(10), String("unused"),
		},
		[]Value{
			Uint(0),
			Uint(1),
			Uint(2),
			Uint(10),
			Uint(100),
			Uint(1000),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[UintKind], PrimitiveTypeMap[NullKind],
		[]Value{
			Uint(0), NullValue,
			Uint(1000), NullValue,
			Uint(1), NullValue,
			Uint(100), NullValue,
			Uint(2), NullValue,
			Uint(10), NullValue,
		},
		[]Value{
			Uint(0),
			Uint(1),
			Uint(2),
			Uint(10),
			Uint(100),
			Uint(1000),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[NullKind], PrimitiveTypeMap[StringKind],
		[]Value{
			NullValue, String("val 1"),
			NullValue, String("val 2"),
			NullValue, String("val 3"),
		},
		[]Value{
			NullValue,
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[IntKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Int(0), String("unused"),
			Int(1000), String("unused"),
			Int(-1), String("unused"),
			Int(100), String("unused"),
			Int(2), String("unused"),
			Int(-10), String("unused"),
		},
		[]Value{
			Int(-10),
			Int(-1),
			Int(0),
			Int(2),
			Int(100),
			Int(1000),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Float(0), String("unused"),
			Float(-30), String("unused"),
			Float(25), String("unused"),
			Float(1002), NullValue,
			Float(-5050), String("unused"),
			Float(23), String("unused"),
		},
		[]Value{
			Float(-5050),
			Float(-30),
			Float(0),
			Float(23),
			Float(25),
			Float(1002),
		},
	)

	uuids := []UUID{
		UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000000"))),
		UUID(uuid.Must(uuid.Parse("00000000-0000-0000-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("10000000-0000-0000-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("10000000-0000-0001-0000-000000000001"))),
		UUID(uuid.Must(uuid.Parse("20000000-0000-0000-0000-000000000001"))),
	}
	testMapOrder(assert, vrw, PrimitiveTypeMap[UUIDKind], PrimitiveTypeMap[StringKind],
		[]Value{
			uuids[4], String("unused"),
			uuids[1], String("unused"),
			uuids[3], String("unused"),
			uuids[0], String("unused"),
			uuids[2], String("unused"),
		},
		[]Value{
			uuids[0],
			uuids[1],
			uuids[2],
			uuids[3],
			uuids[4],
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Float(0.0001), String("unused"),
			Float(0.000001), String("unused"),
			Float(1), String("unused"),
			Float(25.01e3), String("unused"),
			Float(-32.231123e5), String("unused"),
			Float(23), String("unused"),
		},
		[]Value{
			Float(-32.231123e5),
			Float(0.000001),
			Float(0.0001),
			Float(1),
			Float(23),
			Float(25.01e3),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[ValueKind], PrimitiveTypeMap[StringKind],
		[]Value{
			String("a"), String("unused"),
			String("z"), String("unused"),
			String("b"), String("unused"),
			String("y"), String("unused"),
			String("c"), String("unused"),
			String("x"), String("unused"),
		},
		[]Value{
			String("a"),
			String("b"),
			String("c"),
			String("x"),
			String("y"),
			String("z"),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Bool(true), String("unused"),
			Bool(false), String("unused"),
		},
		[]Value{
			Bool(false),
			Bool(true),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[InlineBlobKind], PrimitiveTypeMap[StringKind],
		[]Value{
			InlineBlob([]byte{00, 01, 1}), String("unused"),
			InlineBlob([]byte{00, 01, 9}), String("unused"),
			InlineBlob([]byte{00, 01, 2}), String("unused"),
			InlineBlob([]byte{00, 01, 8}), String("unused"),
			InlineBlob([]byte{00, 01, 3}), String("unused"),
			InlineBlob([]byte{00, 01, 7}), String("unused"),
		},
		[]Value{
			InlineBlob([]byte{00, 01, 1}),
			InlineBlob([]byte{00, 01, 2}),
			InlineBlob([]byte{00, 01, 3}),
			InlineBlob([]byte{00, 01, 7}),
			InlineBlob([]byte{00, 01, 8}),
			InlineBlob([]byte{00, 01, 9}),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[TimestampKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Timestamp(time.Unix(1000, 0).UTC()), String("unused"),
			Timestamp(time.Unix(9000, 0).UTC()), String("unused"),
			Timestamp(time.Unix(2000, 0).UTC()), String("unused"),
			Timestamp(time.Unix(8000, 0).UTC()), String("unused"),
			Timestamp(time.Unix(3000, 0).UTC()), String("unused"),
			Timestamp(time.Unix(7000, 0).UTC()), String("unused"),
		},
		[]Value{
			Timestamp(time.Unix(1000, 0).UTC()),
			Timestamp(time.Unix(2000, 0).UTC()),
			Timestamp(time.Unix(3000, 0).UTC()),
			Timestamp(time.Unix(7000, 0).UTC()),
			Timestamp(time.Unix(8000, 0).UTC()),
			Timestamp(time.Unix(9000, 0).UTC()),
		},
	)

	testMapOrder(assert, vrw,
		PrimitiveTypeMap[DecimalKind], PrimitiveTypeMap[StringKind],
		[]Value{
			Decimal(decimal.RequireFromString("-99.125434")), String("unused"),
			Decimal(decimal.RequireFromString("482.124")), String("unused"),
			Decimal(decimal.RequireFromString("858093.12654")), String("unused"),
			Decimal(decimal.RequireFromString("1")), String("unused"),
			Decimal(decimal.RequireFromString("-99.125432")), String("unused"),
			Decimal(decimal.RequireFromString("0")), String("unused"),
			Decimal(decimal.RequireFromString("-123845")), String("unused"),
			Decimal(decimal.RequireFromString("-99.125433")), String("unused"),
		},
		[]Value{
			Decimal(decimal.RequireFromString("-123845")),
			Decimal(decimal.RequireFromString("-99.125434")),
			Decimal(decimal.RequireFromString("-99.125433")),
			Decimal(decimal.RequireFromString("-99.125432")),
			Decimal(decimal.RequireFromString("0")),
			Decimal(decimal.RequireFromString("1")),
			Decimal(decimal.RequireFromString("482.124")),
			Decimal(decimal.RequireFromString("858093.12654")),
		},
	)
}

func TestMapEmpty(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()
	ctx := context.Background()

	m, err := NewMap(ctx, vrw)
	require.NoError(t, err)
	me := m.Edit()

	m, err = me.Map(ctx)
	require.NoError(t, err)
	me = m.Edit()
	assert.True(m.Empty())

	me.Set(Bool(false), String("hi"))
	m, err = me.Map(ctx)
	require.NoError(t, err)
	me = m.Edit()
	assert.False(m.Empty())

	l, err := NewList(ctx, vrw)
	require.NoError(t, err)
	m2, err := NewMap(ctx, vrw)
	require.NoError(t, err)
	me.Set(l, m2)
	m, err = me.Map(ctx)
	require.NoError(t, err)
	assert.False(m.Empty())
}

func TestMapType(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	emptyMapType := mustType(MakeMapType(mustType(MakeUnionType()), mustType(MakeUnionType())))
	m, err := NewMap(context.Background(), vrw)
	require.NoError(t, err)
	mt, err := TypeOf(m)
	require.NoError(t, err)
	assert.True(mt.Equals(emptyMapType))

	m2, err := m.Edit().Remove(String("B")).Map(context.Background())
	require.NoError(t, err)
	assert.True(emptyMapType.Equals(mustType(TypeOf(m2))))

	tr, err := MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	m2, err = m.Edit().Set(String("A"), Float(1)).Map(context.Background())
	require.NoError(t, err)
	assert.True(tr.Equals(mustType(TypeOf(m2))))

	m2, err = m.Edit().Set(String("B"), Float(2)).Set(String("C"), Float(2)).Map(context.Background())
	require.NoError(t, err)
	assert.True(tr.Equals(mustType(TypeOf(m2))))

	m3, err := m2.Edit().Set(String("A"), Bool(true)).Map(context.Background())
	require.NoError(t, err)
	assert.True(mustType(MakeMapType(PrimitiveTypeMap[StringKind], mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))).Equals(mustType(TypeOf(m3))), mustString(mustType(TypeOf(m3)).Describe(context.Background())))
	m4, err := m3.Edit().Set(Bool(true), Float(1)).Map(context.Background())
	require.NoError(t, err)
	assert.True(mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[StringKind])), mustType(MakeUnionType(PrimitiveTypeMap[BoolKind], PrimitiveTypeMap[FloatKind])))).Equals(mustType(TypeOf(m4))))
}

func TestMapChunks(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	l1, err := NewMap(context.Background(), vrw, Float(0), Float(1))
	require.NoError(t, err)
	c1 := getChunks(l1)
	assert.Len(c1, 0)

	ref1, err := NewRef(Float(0), Format_7_18)
	require.NoError(t, err)
	l2, err := NewMap(context.Background(), vrw, ref1, Float(1))
	require.NoError(t, err)
	c2 := getChunks(l2)
	assert.Len(c2, 1)

	ref2, err := NewRef(Float(1), Format_7_18)
	require.NoError(t, err)
	l3, err := NewMap(context.Background(), vrw, Float(0), ref2)
	require.NoError(t, err)
	c3 := getChunks(l3)
	assert.Len(c3, 1)
}

func TestMapFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)

	vrw := newTestValueStore()

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		kvs = append(kvs, Float(i), Float(i+1))
	}

	m, err := NewMap(context.Background(), vrw, kvs...)
	require.NoError(t, err)
	assert.Equal(deriveCollectionHeight(m), getRefHeightOfCollection(m))
}

func TestMapRefOfStructFirstNNumbers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode.")
	}
	assert := assert.New(t)
	vs := newTestValueStore()

	kvs := []Value{}
	for i := 0; i < testMapSize; i++ {
		k, err := vs.WriteValue(context.Background(), mustValue(NewStruct(Format_7_18, "num", StructData{"n": Float(i)})))
		require.NoError(t, err)
		v, err := vs.WriteValue(context.Background(), mustValue(NewStruct(Format_7_18, "num", StructData{"n": Float(i + 1)})))
		require.NoError(t, err)
		assert.NotNil(k)
		assert.NotNil(v)
		kvs = append(kvs, k, v)
	}

	m, err := NewMap(context.Background(), vs, kvs...)
	require.NoError(t, err)
	// height + 1 because the leaves are Ref values (with height 1).
	assert.Equal(deriveCollectionHeight(m)+1, getRefHeightOfCollection(m))
}

func TestMapModifyAfterRead(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	m := getTestNativeOrderMap(2, vs).toMap(vs)
	// Drop chunk values.

	ref, err := vs.WriteValue(context.Background(), m)
	require.NoError(t, err)
	v, err := vs.ReadValue(context.Background(), ref.TargetHash())
	require.NoError(t, err)
	m = v.(Map)

	// Modify/query. Once upon a time this would crash.
	fst, fstval, err := m.First(context.Background())
	require.NoError(t, err)
	m, err = m.Edit().Remove(fst).Map(context.Background())
	require.NoError(t, err)
	assert.False(m.Has(context.Background(), fst))

	fst2, _, err := m.First(context.Background())
	require.NoError(t, err)
	assert.True(m.Has(context.Background(), fst2))

	m, err = m.Edit().Set(fst, fstval).Map(context.Background())
	require.NoError(t, err)
	assert.True(m.Has(context.Background(), fst))
}

func TestMapTypeAfterMutations(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	test := func(n int, c interface{}) {
		values := make([]Value, 2*n)
		for i := 0; i < n; i++ {
			values[2*i] = Float(i)
			values[2*i+1] = Float(i)
		}

		m, err := NewMap(context.Background(), vrw, values...)
		require.NoError(t, err)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(mustType(TypeOf(m)).Equals(mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[FloatKind]))))

		m, err = m.Edit().Set(String("a"), String("a")).Map(context.Background())
		require.NoError(t, err)
		assert.Equal(m.Len(), uint64(n+1))
		assert.IsType(c, m.asSequence())
		assert.True(mustType(TypeOf(m)).Equals(mustType(MakeMapType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])), mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind]))))))

		m, err = m.Edit().Remove(String("a")).Map(context.Background())
		require.NoError(t, err)
		assert.Equal(m.Len(), uint64(n))
		assert.IsType(c, m.asSequence())
		assert.True(mustType(TypeOf(m)).Equals(mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[FloatKind]))))
	}

	test(10, mapLeafSequence{})
	test(8000, metaSequence{})
}

func TestCompoundMapWithValuesOfEveryType(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	v := Float(42)
	kvs := []Value{
		// Values
		Bool(true), v,
		Float(0), v,
		String("hello"), v,
		mustValue(NewBlob(context.Background(), vrw, bytes.NewBufferString("buf"))), v,
		mustValue(NewSet(context.Background(), vrw, Bool(true))), v,
		mustValue(NewList(context.Background(), vrw, Bool(true))), v,
		mustValue(NewMap(context.Background(), vrw, Bool(true), Float(0))), v,
		mustValue(NewStruct(Format_7_18, "", StructData{"field": Bool(true)})), v,
		// Refs of values
		mustValue(NewRef(Bool(true), Format_7_18)), v,
		mustValue(NewRef(Float(0), Format_7_18)), v,
		mustValue(NewRef(String("hello"), Format_7_18)), v,
		mustValue(NewRef(mustValue(NewBlob(context.Background(), vrw, bytes.NewBufferString("buf"))), Format_7_18)), v,
		mustValue(NewRef(mustValue(NewSet(context.Background(), vrw, Bool(true))), Format_7_18)), v,
		mustValue(NewRef(mustValue(NewList(context.Background(), vrw, Bool(true))), Format_7_18)), v,
		mustValue(NewRef(mustValue(NewMap(context.Background(), vrw, Bool(true), Float(0))), Format_7_18)), v,
		mustValue(NewRef(mustValue(NewStruct(Format_7_18, "", StructData{"field": Bool(true)})), Format_7_18)), v,
	}

	m, err := NewMap(context.Background(), vrw, kvs...)
	require.NoError(t, err)
	for i := 1; m.asSequence().isLeaf(); i++ {
		k := Float(i)
		kvs = append(kvs, k, v)
		m, err = m.Edit().Set(k, v).Map(context.Background())
		require.NoError(t, err)
	}

	assert.Equal(len(kvs)/2, int(m.Len()))
	fk, fv, err := m.First(context.Background())
	require.NoError(t, err)
	assert.True(bool(fk.(Bool)))
	assert.True(v.Equals(fv))

	for i, keyOrValue := range kvs {
		if i%2 == 0 {
			assert.True(m.Has(context.Background(), keyOrValue))
			retrievedVal, ok, err := m.MaybeGet(context.Background(), keyOrValue)
			assert.True(ok)
			require.NoError(t, err)
			assert.True(v.Equals(retrievedVal))
		} else {
			assert.True(v.Equals(keyOrValue))
		}
	}

	for len(kvs) > 0 {
		k := kvs[0]
		kvs = kvs[2:]
		m, err = m.Edit().Remove(k).Map(context.Background())
		require.NoError(t, err)
		assert.False(m.Has(context.Background(), k))
		assert.Equal(len(kvs)/2, int(m.Len()))
	}
}

func TestMapRemoveLastWhenNotLoaded(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(m Map) (Map, error) {
		ref, err := vs.WriteValue(context.Background(), m)

		if err != nil {
			return EmptyMap, err
		}

		v, err := vs.ReadValue(context.Background(), ref.TargetHash())

		if err != nil {
			return EmptyMap, err
		}

		return v.(Map), nil
	}

	tm := getTestNativeOrderMap(4, vs)
	nm := tm.toMap(vs)

	for len(tm.entries.entries) > 0 {
		entr := tm.entries.entries
		last := entr[len(entr)-1]
		entr = entr[:len(entr)-1]
		tm.entries.entries = entr
		m, err := nm.Edit().Remove(last.key).Map(context.Background())
		require.NoError(t, err)
		nm, err = reload(m)
		require.NoError(t, err)
		assert.True(tm.toMap(vs).Equals(nm))
	}
}

func TestMapIterFrom(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	test := func(m Map, start, end Value) ValueSlice {
		res := ValueSlice{}
		err := m.IterFrom(context.Background(), start, func(k, v Value) (bool, error) {
			isLess, err := end.Less(Format_7_18, k)

			if err != nil {
				return false, err
			}

			if isLess {
				return true, nil
			}
			res = append(res, k, v)
			return false, nil
		})
		require.NoError(t, err)
		return res
	}

	kvs := generateNumbersAsValuesFromToBy(-50, 50, 1)
	m1, err := NewMap(context.Background(), vrw, kvs...)
	require.NoError(t, err)
	assert.True(kvs.Equals(test(m1, nil, Float(1000))))
	assert.True(kvs.Equals(test(m1, Float(-1000), Float(1000))))
	assert.True(kvs.Equals(test(m1, Float(-50), Float(1000))))
	assert.True(kvs[2:].Equals(test(m1, Float(-49), Float(1000))))
	assert.True(kvs[2:].Equals(test(m1, Float(-48), Float(1000))))
	assert.True(kvs[4:].Equals(test(m1, Float(-47), Float(1000))))
	assert.True(kvs[98:].Equals(test(m1, Float(48), Float(1000))))
	assert.True(kvs[0:0].Equals(test(m1, Float(100), Float(1000))))
	assert.True(kvs[50:60].Equals(test(m1, Float(0), Float(8))))
}

func TestMapAt(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	values := []Value{Bool(false), Float(42), String("a"), String("b"), String("c"), String("d")}
	m, err := NewMap(context.Background(), vrw, values...)
	require.NoError(t, err)

	for i := 0; i < len(values); i += 2 {
		k, v, err := m.At(context.Background(), uint64(i/2))
		require.NoError(t, err)
		assert.Equal(values[i], k)
		assert.Equal(values[i+1], v)
	}

	assert.Panics(func() {
		m.At(context.Background(), 42)
	})
}

func TestMapWithStructShouldHaveOptionalFields(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	list := mustMap(NewMap(context.Background(), vrw,
		String("one"),
		mustValue(NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(1),
		})),
		String("two"),
		mustValue(NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		}))),
	)
	assert.True(
		mustType(MakeMapType(PrimitiveTypeMap[StringKind],
			mustType(MakeStructType("Foo",
				StructField{"a", PrimitiveTypeMap[FloatKind], false},
				StructField{"b", PrimitiveTypeMap[StringKind], true},
			)),
		)).Equals(mustType(TypeOf(list))))

	// transpose
	list = mustMap(NewMap(context.Background(), vrw,
		mustValue(NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(1),
		})),
		String("one"),
		mustValue(NewStruct(Format_7_18, "Foo", StructData{
			"a": Float(2),
			"b": String("bar"),
		})),
		String("two"),
	))
	assert.True(
		mustType(MakeMapType(
			mustType(MakeStructType("Foo",
				StructField{"a", PrimitiveTypeMap[FloatKind], false},
				StructField{"b", PrimitiveTypeMap[StringKind], true},
			)),
			PrimitiveTypeMap[StringKind],
		)).Equals(mustType(TypeOf(list))))

}

func TestMapWithNil(t *testing.T) {
	vrw := newTestValueStore()
	assert.Panics(t, func() {
		NewMap(context.Background(), nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(context.Background(), vrw, Float(42), nil)
	})
	assert.Panics(t, func() {
		NewMap(context.Background(), vrw, String("a"), String("b"), nil, Float(42))
	})
	assert.Panics(t, func() {
		NewSet(context.Background(), vrw, String("a"), String("b"), Float(42), nil)
	})
}

func TestVisitMapLevelOrderSized(t *testing.T) {
	smallTestChunks()
	defer normalProductionChunks()

	assert := assert.New(t)

	tests := []struct {
		description string
		mapSize     int
		batchSize   int
	}{
		{
			description: "large batch",
			mapSize:     testMapSize * 4,
			batchSize:   200,
		},
		{
			description: "medium batch",
			mapSize:     testMapSize * 2,
			batchSize:   20,
		},
		{
			description: "small batch",
			mapSize:     testMapSize,
			batchSize:   2,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			vrw := newTestValueStore()
			kvs := []Value{}
			for i := 0; i < test.mapSize; i++ {
				kvs = append(kvs, Float(i), Float(i+1))
			}

			m, err := NewMap(context.Background(), vrw, kvs...)
			d.PanicIfError(err)

			expectedChunkHashes := make([]hash.Hash, 0)
			_, _, err = VisitMapLevelOrder(m, func(h hash.Hash) (int64, error) {
				expectedChunkHashes = append(expectedChunkHashes, h)
				return 0, nil
			})
			d.PanicIfError(err)

			actualChunkHashes := make([]hash.Hash, 0)
			_, _, err = VisitMapLevelOrderSized([]Map{m}, test.batchSize, func(h hash.Hash) (int64, error) {
				actualChunkHashes = append(actualChunkHashes, h)
				return 0, nil
			})
			d.PanicIfError(err)
			sort.Slice(expectedChunkHashes, func(i, j int) bool {
				return expectedChunkHashes[i].Less(expectedChunkHashes[j])
			})
			sort.Slice(actualChunkHashes, func(i, j int) bool {
				return actualChunkHashes[i].Less(actualChunkHashes[j])
			})
			assert.Equal(expectedChunkHashes, actualChunkHashes)
		})
	}
}

func TestMapIndexForKey(t *testing.T) {
	tests := []struct {
		name       string
		numEntries int64
		numChecks  int
	}{
		{name: "multiple levels", numEntries: 1_000_000, numChecks: 10_000},
		{name: "leaves only", numEntries: 100, numChecks: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kvps := make([]Value, 2*test.numEntries)
			for i := int64(0); i < test.numEntries; i++ {
				kvps[i*2] = Int(i)
				kvps[i*2+1] = NullValue
			}

			ctx := context.Background()
			vrw := newTestValueStore()

			m, err := NewMap(ctx, vrw, kvps...)
			require.NoError(t, err)

			for i := 0; i < test.numChecks; i++ {
				k := rand.Int63n(test.numEntries)
				idx, err := m.IndexForKey(ctx, Int(k))
				require.NoError(t, err)
				require.Equal(t, k, idx)
			}

			// Test before start
			idx, err := m.IndexForKey(ctx, Int(-1))
			require.NoError(t, err)
			require.Equal(t, int64(0), idx)

			// Test after end
			idx, err = m.IndexForKey(ctx, Int(test.numEntries+1))
			require.NoError(t, err)
			require.Equal(t, int64(test.numEntries), idx)
		})
	}
}
