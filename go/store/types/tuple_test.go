// Copyright 2019 Liquidata, Inc.
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

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
)

const (
	ZeroUUID = "00000000-0000-0000-0000-000000000000"
	OneUUID  = "11111111-1111-1111-1111-111111111111"
)

func TestTupleEquality(t *testing.T) {
	values := []Value{String("aoeu"), Int(-1234), Uint(1234)}
	tpl, err := NewTuple(Format_7_18, values...)
	assert.NoError(t, err)

	if !tpl.Equals(tpl) {
		t.Error("Tuple not equal to itself")
	}

	id := UUID(uuid.MustParse(ZeroUUID))
	tpl2, err := tpl.Append(id)
	assert.NoError(t, err)
	idIdx := tpl2.Len() - 1

	if tpl.Equals(tpl2) {
		t.Error("Tuples should not be equal")
	}

	temp, err := tpl2.Set(idIdx, id)
	assert.NoError(t, err)
	temp, err = temp.Set(0, String("aoeu"))
	assert.NoError(t, err)
	temp, err = temp.Set(1, Int(-1234))
	assert.NoError(t, err)
	tpl3, err := temp.Set(2, Uint(1234))
	assert.NoError(t, err)

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	tpl3, err = tpl2.Set(0, String("aoeu"))
	assert.NoError(t, err)

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	tpl3, err = tpl2.Set(1, Int(-1234))
	assert.NoError(t, err)

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	tpl3, err = tpl2.Set(2, Uint(1234))
	assert.NoError(t, err)

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	tpl3, err = tpl2.Set(idIdx, id)
	assert.NoError(t, err)

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	if !tpl2.Equals(tpl3) {
		t.Error("")
	}

	idVal, err := tpl3.Get(idIdx)
	assert.NoError(t, err)

	if idVal.Kind() != UUIDKind {
		t.Error("Unexpected type")
	}

	err = tpl3.IterFields(func(index uint64, value Value) (stop bool, err error) {
		equal := false
		switch index {
		case 0, 1, 2:
			equal = values[index].Equals(value)
		case 3:
			equal = id.Equals(value)
		}

		if !equal {
			t.Errorf("Unexpected tuple value at index %d", index)
		}

		return false, nil
	})

	assert.NoError(t, err)
}

func TestTupleLess(t *testing.T) {
	tests := []struct {
		vals1    []Value
		vals2    []Value
		expected bool
	}{
		{
			[]Value{String("equal")},
			[]Value{String("equal")},
			false,
		},
		{
			[]Value{String("abc"), Int(1234)},
			[]Value{String("abc"), Int(1234)},
			false,
		},
		{
			[]Value{String("abc"), Int(1234)},
			[]Value{String("abc"), Int(1235)},
			true,
		},
		{
			[]Value{String("abc"), Int(1235)},
			[]Value{String("abc"), Int(1234)},
			false,
		},
		{
			[]Value{String("abc"), Int(1234)},
			[]Value{String("abc")},
			false,
		},
		{
			[]Value{String("abc")},
			[]Value{String("abc"), Int(1234)},
			true,
		},
		{
			[]Value{UUID(uuid.MustParse(ZeroUUID)), String("abc")},
			[]Value{UUID(uuid.MustParse(OneUUID)), String("abc")},
			true,
		},
	}

	for _, test := range tests {
		tpl1, err := NewTuple(Format_7_18, test.vals1...)

		assert.NoError(t, err)

		tpl2, err := NewTuple(Format_7_18, test.vals2...)
		assert.NoError(t, err)

		actual, err := tpl1.Less(Format_7_18, tpl2)
		assert.NoError(t, err)

		if actual != test.expected {
			t.Error("tpl1:", mustString(EncodedValue(context.Background(), tpl1)), "tpl2:", mustString(EncodedValue(context.Background(), tpl2)), "expected", test.expected, "actual:", actual)
		}
	}
}
