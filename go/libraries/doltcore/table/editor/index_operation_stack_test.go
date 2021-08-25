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

package editor

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestIndexOperationStack(t *testing.T) {
	ios := &indexOperationStack{}
	require.True(t, len(ios.entries) >= 2) // Entries should always at least have a length of 2

	ios.Push(true, iosTuple(t, 100, 100), iosTuple(t, 100), iosTuple(t, 0))
	entry, ok := ios.Pop()
	require.True(t, ok)
	iosTupleComp(t, entry.fullKey, 100, 100)
	iosTupleComp(t, entry.partialKey, 100)
	iosTupleComp(t, entry.value, 0)
	require.True(t, entry.isInsert)
	_, ok = ios.Pop()
	require.False(t, ok)

	for i := 0; i < len(ios.entries); i++ {
		ios.Push(false, iosTuple(t, i, i), iosTuple(t, i), iosTuple(t, i*2))
	}
	for i := len(ios.entries) - 1; i >= 0; i-- {
		entry, ok = ios.Pop()
		require.True(t, ok)
		iosTupleComp(t, entry.fullKey, i, i)
		iosTupleComp(t, entry.partialKey, i)
		iosTupleComp(t, entry.partialKey, i*2)
		require.False(t, entry.isInsert)
	}
	_, ok = ios.Pop()
	require.False(t, ok)

	for i := 0; i < (len(ios.entries)*2)+1; i++ {
		ios.Push(true, iosTuple(t, i, i), iosTuple(t, i), iosTuple(t, i*2))
	}
	for i := len(ios.entries) - 1; i >= 0; i-- {
		entry, ok = ios.Pop()
		require.True(t, ok)
		val := ((len(ios.entries) * 2) + 1) - i
		iosTupleComp(t, entry.fullKey, val, val)
		iosTupleComp(t, entry.partialKey, val)
		iosTupleComp(t, entry.value, val*2)
		require.True(t, entry.isInsert)
	}
	_, ok = ios.Pop()
	require.False(t, ok)
}

func iosTuple(t *testing.T, vals ...int) types.Tuple {
	typeVals := make([]types.Value, len(vals))
	for i, val := range vals {
		typeVals[i] = types.Int(val)
	}
	tpl, err := types.NewTuple(types.Format_Default, typeVals...)
	if err != nil {
		require.NoError(t, err)
	}
	return tpl
}

func iosTupleComp(t *testing.T, tpl types.Tuple, vals ...int) bool {
	if tpl.Len() != uint64(len(vals)) {
		return false
	}
	iter, err := tpl.Iterator()
	require.NoError(t, err)
	var i uint64
	var val types.Value
	for i, val, err = iter.Next(); i < uint64(len(vals)) && err == nil; i, val, err = iter.Next() {
		if !types.Int(vals[i]).Equals(val) {
			return false
		}
	}
	require.NoError(t, err)
	return true
}
