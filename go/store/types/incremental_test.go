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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
)

func getTestVals(vrw ValueReadWriter) []Value {
	return []Value{
		Bool(true),
		Float(1),
		String("hi"),
		mustBlob(NewBlob(context.Background(), vrw, bytes.NewReader([]byte("hi")))),
		// compoundBlob
		mustValue(NewSet(context.Background(), vrw, String("hi"))),
		mustList(NewList(context.Background(), vrw, String("hi"))),
		mustValue(NewMap(context.Background(), vrw, String("hi"), String("hi"))),
	}
}

func isEncodedOutOfLine(v Value) int {
	switch v.(type) {
	case Ref:
		return 1
	}
	return 0
}

func TestIncrementalLoadList(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expected, err := NewList(context.Background(), vs, getTestVals(vs)...)
	require.NoError(t, err)
	ref, err := vs.WriteValue(context.Background(), expected)
	require.NoError(t, err)
	hash := ref.TargetHash()
	rt, err := vs.Root(context.Background())
	require.NoError(t, err)
	_, err = vs.Commit(context.Background(), rt, rt)
	require.NoError(t, err)

	actualVar, err := vs.ReadValue(context.Background(), hash)
	require.NoError(t, err)
	actual := actualVar.(List)

	expectedCount := cs.Reads()
	assert.Equal(1, expectedCount)
	// There will be one read per chunk.
	chunkReads := make([]int, expected.Len())
	for i := uint64(0); i < expected.Len(); i++ {
		v, err := actual.Get(context.Background(), i)
		require.NoError(t, err)
		v2, err := expected.Get(context.Background(), i)
		require.NoError(t, err)
		assert.True(v2.Equals(v))

		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads())

		// Do it again to make sure multiple derefs don't do multiple loads.
		_, err = actual.Get(context.Background(), i)
		require.NoError(t, err)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads())
	}
}

func SkipTestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expected, err := NewSet(context.Background(), vs, getTestVals(vs)...)
	require.NoError(t, err)
	ref, err := vs.WriteValue(context.Background(), expected)
	require.NoError(t, err)
	refHash := ref.TargetHash()

	actualVar, err := vs.ReadValue(context.Background(), refHash)
	require.NoError(t, err)
	actual := actualVar.(Set)

	expectedCount := cs.Reads()
	assert.Equal(1, expectedCount)
	err = actual.Iter(context.Background(), func(v Value) (bool, error) {
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads())
		return false, nil
	})

	require.NoError(t, err)
}

func SkipTestIncrementalLoadMap(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expected, err := NewMap(context.Background(), vs, getTestVals(vs)...)
	require.NoError(t, err)
	ref, err := vs.WriteValue(context.Background(), expected)
	require.NoError(t, err)
	refHash := ref.TargetHash()

	actualVar, err := vs.ReadValue(context.Background(), refHash)
	require.NoError(t, err)
	actual := actualVar.(Map)

	expectedCount := cs.Reads()
	assert.Equal(1, expectedCount)
	err = actual.Iter(context.Background(), func(k, v Value) (bool, error) {
		expectedCount += isEncodedOutOfLine(k)
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads())
		return false, nil
	})
	require.NoError(t, err)
}

func SkipTestIncrementalAddRef(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expectedItem := Float(42)
	ref, err := vs.WriteValue(context.Background(), expectedItem)
	require.NoError(t, err)

	expected, err := NewList(context.Background(), vs, ref)
	require.NoError(t, err)
	ref, err = vs.WriteValue(context.Background(), expected)
	require.NoError(t, err)
	actualVar, err := vs.ReadValue(context.Background(), ref.TargetHash())
	require.NoError(t, err)

	assert.Equal(1, cs.Reads())
	assert.True(expected.Equals(actualVar))

	actual := actualVar.(List)
	actualItem, err := actual.Get(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(2, cs.Reads())
	assert.True(expectedItem.Equals(actualItem))

	// do it again to make sure caching works.
	actualItem, err = actual.Get(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(2, cs.Reads())
	assert.True(expectedItem.Equals(actualItem))
}
