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
	"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
}

const testCollectionSize = 100000

func genTestBlob() (Blob, []byte) {
	var buffer bytes.Buffer
	for i := 0; i < testCollectionSize; i += 1 {
		for _, v := range seedData {
			buffer.WriteString(fmt.Sprintf("%d%s", i, v))
		}
	}
	blob := NewBlob(&buffer)
	return blob, buffer.Bytes()
}

func TestIterBlob(t *testing.T) {
	testIter := func(t *testing.T, blob Blob, expected []byte, start int) {
		assert := assert.New(t)
		expected = expected[start:]
		iter := newSequenceIterator(blob.seq, uint64(start))
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
		assert.InDelta(1.0, iter.readAheadHitRate(), 0.01)
	}

	blob, expected := genTestBlob()

	testIter(t, blob, expected, 0)
	testIter(t, blob, expected, len(expected)/2)
}

func TestIterList(t *testing.T) {
	genList := func() (List, []string) {
		var buffer []string
		var lbuffer []Value

		list := NewList()

		for i := 0; i < testCollectionSize; i += 1 {
			for _, v := range seedData {
				s := fmt.Sprintf("%d%s", i, v)
				buffer = append(buffer, s)
				lbuffer = append(lbuffer, String(s))
			}
		}
		list = list.Append(lbuffer...)
		return list, buffer

	}
	testIter := func(t *testing.T, list List, expected []string, start int) {
		assert := assert.New(t)
		expected = expected[start:]
		iter := newSequenceIterator(list.seq, uint64(start))
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
