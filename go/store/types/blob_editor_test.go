// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"math/rand"
	"testing"

	"io/ioutil"

	"bytes"

	"io"

	"github.com/liquidata-inc/ld/dolt/go/store/go/chunks"
	"github.com/stretchr/testify/assert"
)

func TestBlobReadWriteFuzzer(t *testing.T) {
	rounds := 1024
	operations := 512
	flushEvery := 16
	maxInsertCount := uint64(64)

	ts := &chunks.TestStorage{}
	cs := ts.NewView()
	vs := newValueStoreWithCacheAndPending(cs, 0, 0)

	r := rand.New(rand.NewSource(0))
	nextRandInt := func(from, to uint64) uint64 {
		return from + uint64(float64(to-from)*r.Float64())
	}

	for i := 0; i < rounds; i++ {
		b := NewBlob(context.Background(), vs)

		f, _ := ioutil.TempFile("", "buff")
		be := b.Edit()

		for j := 0; j < operations; j++ {
			if j%2 == 1 {
				// random read
				idx := nextRandInt(0, be.Len())
				l := nextRandInt(0, be.Len()-idx)
				f.Seek(int64(idx), 0)
				be.Seek(int64(idx), 0)

				ex := make([]byte, l)
				ac := make([]byte, l)

				f.Read(ex)
				be.Read(context.Background(), ac)
				assert.True(t, bytes.Equal(ex, ac))
			} else {
				// randon write
				idx := nextRandInt(0, be.Len())
				f.Seek(int64(idx), 0)
				be.Seek(int64(idx), 0)

				l := nextRandInt(0, maxInsertCount)
				data, err := ioutil.ReadAll(&io.LimitedReader{r, int64(l)})
				assert.NoError(t, err)
				f.Write(data)
				be.Write(data)
			}
			if j%flushEvery == 0 {
				// Flush
				b = be.Blob(context.Background())
				be = b.Edit()
			}
		}

		f.Sync()
		b = be.Blob(context.Background())

		f.Seek(0, 0)
		info, err := f.Stat()
		assert.NoError(t, err)
		assert.True(t, uint64(info.Size()) == b.Len())
		expect, err := ioutil.ReadAll(f)
		assert.NoError(t, err)

		actual := make([]byte, b.Len())
		b.ReadAt(context.Background(), actual, 0)

		assert.True(t, bytes.Equal(expect, actual))
	}
}
