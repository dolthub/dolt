// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/attic-labs/testify/assert"
)

var seedData = []string{
	"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
}

const testCollectionSize = 100000

func genTestBlob() (Blob, []byte) {
	var buffer bytes.Buffer
	smallTestChunks()
	defer normalProductionChunks()
	for i := 0; buffer.Len() < testCollectionSize; i += 1 {
		v := seedData[i%len(seedData)]
		buffer.WriteString(fmt.Sprintf("%d%s", i, v))
	}
	raw := buffer.Bytes()
	blob := NewBlob(&buffer)
	return blob, raw
}

func assertMinDepth(assert *assert.Assertions, iter *sequenceIterator, min int) {
	depth := iter.cursor.depth()
	assert.Condition(func() bool { return depth >= min }, "depth less the min depth: %d >= %d", depth, min)
}

func TestIterBlob(t *testing.T) {
	testIter := func(t *testing.T, blob Blob, expected []byte, start int) {
		assert := assert.New(t)
		expected = expected[start:]
		iter := newSequenceIterator(blob.seq, uint64(start))
		// need 4 levels to exercise parent traversal during read-ahead
		assertMinDepth(assert, iter, 4)
		var actual []byte
		for iter.hasMore() {
			actual = append(actual, iter.item().(byte))
			iter.advance(1)
		}
		assert.Equal(len(expected), len(actual))
		assert.Equal(expected, actual)
		// delta normally 0 but may be more in rare case where the same
		// (chunkIdx, hash) pair is repeated in different chunks. A lower
		// hit rate likely indicates a bug.
		assert.InDelta(1.0, iter.readAheadHitRate(), 0.05)
	}
	blob, expected := genTestBlob()
	testIter(t, blob, expected, 0)
	testIter(t, blob, expected, len(expected)/2)
}

func TestIterList(t *testing.T) {
	genList := func() (List, []string) {
		var buffer []string
		var lbuffer []Value

		smallTestChunks()
		defer normalProductionChunks()
		list := NewList()

		for i := 0; len(buffer) < testCollectionSize; i += 1 {
			v := seedData[i%len(seedData)]
			s := fmt.Sprintf("%d%s", i, v)
			buffer = append(buffer, s)
			lbuffer = append(lbuffer, String(s))
		}
		list = list.Append(lbuffer...)
		return list, buffer

	}
	testIter := func(t *testing.T, list List, expected []string, start int) {
		assert := assert.New(t)
		expected = expected[start:]
		iter := newSequenceIterator(list.seq, uint64(start))
		assertMinDepth(assert, iter, 4)
		actual := []string{}
		for iter.hasMore() {
			actual = append(actual, string(iter.item().(String)))
			iter.advance(1)
		}
		assert.Equal(len(expected), len(actual))
		assert.Equal(expected, actual)
	}
	list, expected := genList()
	testIter(t, list, expected, 0)
	testIter(t, list, expected, len(expected)/2)
}
