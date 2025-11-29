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

package val

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/pool"
)

var testPool = pool.NewBuffPool()

func TestNewTuple(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields()
		tup := NewTuple(testPool, fields...)
		for i, field := range fields {
			assert.Equal(t, field, tup.GetField(i))
		}
	}
}

func TestTuplePrefix(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields()
		full := NewTuple(testPool, fields...)
		for i := 0; i <= len(fields); i++ {
			exp := NewTuple(testPool, fields[:i]...)
			act := tuplePrefix(testPool, full, i)
			assert.Equal(t, exp, act)
		}
	}
}

func TestTupleSuffix(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields()
		full := NewTuple(testPool, fields...)
		for i := 0; i <= full.Count(); i++ {
			exp := NewTuple(testPool, fields[i:]...)
			act := tupleSuffix(testPool, full, full.Count()-i)
			assert.Equal(t, exp, act)
		}
	}
}

func randomTuples(numTuples int) []Tuple {
	tuples := make([]Tuple, numTuples)
	for i := range tuples {
		fields := randomByteFields()
		tuples[i] = NewTuple(testPool, fields...)
	}
	return tuples
}

// BenchmarkTupleCount-14    	 4612776	       239.2 ns/op
func BenchmarkTupleCount(b *testing.B) {
	tuples := randomTuples(1024)

	b.ResetTimer()
	var res int
	for i := 0; i < b.N; i++ {
		for _, tuple := range tuples {
			res += tuple.Count()
		}
	}
}

// BenchmarkGetField-14    	   42997	     26963 ns/op
func BenchmarkGetField(b *testing.B) {
	tuples := randomTuples(1024)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _, tup := range tuples {
			for i := 0; i < tup.Count(); i++ {
				_ = tup.GetField(i)
			}
		}
	}
}

func randomByteFields() (fields [][]byte) {
	fields = make([][]byte, rand.Intn(19)+1)
	for i := range fields {
		if rand.Uint32()%4 == 0 {
			// 25% NULL
			fields[i] = nil
			continue
		}
		fields[i] = make([]byte, rand.Intn(19)+1)
		rand.Read(fields[i])
	}
	return
}

func tuplePrefix(pool pool.BuffPool, tup Tuple, k int) Tuple {
	cnt := tup.Count()
	if k >= cnt {
		return tup
	}
	for k > 0 && tup.FieldIsNull(k-1) {
		k-- // trim NULL suffix
	}
	if k == 0 {
		return EmptyTuple
	}

	stop, _ := tup.GetOffset(k)
	prefix, offs := allocateTuple(pool, ByteSize(stop), k)
	split := ByteSize(len(tup)) - uint16Size*ByteSize(cnt)

	copy(prefix, tup[:stop])
	copy(offs, tup[split:])
	return prefix
}

func tupleSuffix(pool pool.BuffPool, tup Tuple, k int) Tuple {
	cnt := tup.Count()
	if k == 0 {
		return EmptyTuple
	} else if k >= cnt {
		return tup
	}
	fields := make([][]byte, k)
	for i := range fields {
		fields[i] = tup.GetField((cnt - k) + i)
	}
	return NewTuple(pool, fields...)
}
