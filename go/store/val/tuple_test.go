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

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/stretchr/testify/assert"
)

var testPool = pool.NewBuffPool()

func TestNewTuple(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields(t)
		tup := NewTuple(testPool, fields...)
		for i, field := range fields {
			assert.Equal(t, field, tup.GetField(i))
		}
	}
}

func TestTuplePrefix(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields(t)
		full := NewTuple(testPool, fields...)
		for i := 0; i <= len(fields); i++ {
			exp := NewTuple(testPool, fields[:i]...)
			act := TuplePrefix(testPool, full, i)
			assert.Equal(t, exp, act)
		}
	}
}

func TestTupleSuffix(t *testing.T) {
	for n := 0; n < 1024; n++ {
		fields := randomByteFields(t)
		full := NewTuple(testPool, fields...)
		for i := 0; i <= full.Count(); i++ {
			exp := NewTuple(testPool, fields[i:]...)
			act := TupleSuffix(testPool, full, full.Count()-i)
			assert.Equal(t, exp, act)
		}
	}
}

func randomByteFields(t *testing.T) (fields [][]byte) {
	fields = make([][]byte, rand.Intn(19)+1)
	assert.True(t, len(fields) > 0)
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
