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

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
)

func getTestVals(vrw ValueReadWriter) []Value {
	return []Value{
		Bool(true),
		Float(1),
		String("hi"),
		NewBlob(context.Background(), vrw, bytes.NewReader([]byte("hi"))),
		// compoundBlob
		NewSet(context.Background(), vrw, String("hi")),
		NewList(context.Background(), vrw, String("hi")),
		NewMap(context.Background(), vrw, String("hi"), String("hi")),
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

	expected := NewList(context.Background(), vs, getTestVals(vs)...)
	hash := vs.WriteValue(context.Background(), expected).TargetHash()
	rt, err := vs.Root(context.Background())
	assert.NoError(err)
	_, err = vs.Commit(context.Background(), rt, rt)
	assert.NoError(err)

	actualVar := vs.ReadValue(context.Background(), hash)
	actual := actualVar.(List)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	// There will be one read per chunk.
	chunkReads := make([]int, expected.Len())
	for i := uint64(0); i < expected.Len(); i++ {
		v := actual.Get(context.Background(), i)
		assert.True(expected.Get(context.Background(), i).Equals(v))

		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)

		// Do it again to make sure multiple derefs don't do multiple loads.
		_ = actual.Get(context.Background(), i)
		assert.Equal(expectedCount+chunkReads[i], cs.Reads)
	}
}

func SkipTestIncrementalLoadSet(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expected := NewSet(context.Background(), vs, getTestVals(vs)...)
	ref := vs.WriteValue(context.Background(), expected).TargetHash()

	actualVar := vs.ReadValue(context.Background(), ref)
	actual := actualVar.(Set)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	actual.Iter(context.Background(), func(v Value) (stop bool) {
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads)
		return
	})
}

func SkipTestIncrementalLoadMap(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expected := NewMap(context.Background(), vs, getTestVals(vs)...)
	ref := vs.WriteValue(context.Background(), expected).TargetHash()

	actualVar := vs.ReadValue(context.Background(), ref)
	actual := actualVar.(Map)

	expectedCount := cs.Reads
	assert.Equal(1, expectedCount)
	actual.Iter(context.Background(), func(k, v Value) (stop bool) {
		expectedCount += isEncodedOutOfLine(k)
		expectedCount += isEncodedOutOfLine(v)
		assert.Equal(expectedCount, cs.Reads)
		return
	})
}

func SkipTestIncrementalAddRef(t *testing.T) {
	assert := assert.New(t)
	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := NewValueStore(cs)

	expectedItem := Float(42)
	ref := vs.WriteValue(context.Background(), expectedItem)

	expected := NewList(context.Background(), vs, ref)
	ref = vs.WriteValue(context.Background(), expected)
	actualVar := vs.ReadValue(context.Background(), ref.TargetHash())

	assert.Equal(1, cs.Reads)
	assert.True(expected.Equals(actualVar))

	actual := actualVar.(List)
	actualItem := actual.Get(context.Background(), 0)
	assert.Equal(2, cs.Reads)
	assert.True(expectedItem.Equals(actualItem))

	// do it again to make sure caching works.
	actualItem = actual.Get(context.Background(), 0)
	assert.Equal(2, cs.Reads)
	assert.True(expectedItem.Equals(actualItem))
}
