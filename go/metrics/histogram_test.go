// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package metrics

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func TestHistogramBucketValue(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	assert.Equal(uint64(1<<0), h.bucketVal(0))
	assert.Equal(uint64(1<<1), h.bucketVal(1))
	assert.Equal(uint64(1<<2), h.bucketVal(2))
	assert.Equal(uint64(1<<32), h.bucketVal(32))
	assert.Equal(uint64(1<<40), h.bucketVal(40))
}

func TestHistogramBasic(t *testing.T) {

	assert := assert.New(t)

	h := Histogram{}

	h.Sample(1)
	h.Sample(1)
	assert.Equal(uint64(2), h.buckets[0])
	assert.Equal(uint64(3), h.bucketSum(0)) // bucket 0's mean value is 1.5

	h.Sample(2)
	h.Sample(3)
	assert.Equal(uint64(2), h.buckets[1])
	assert.Equal(uint64(6), h.bucketSum(1)) // bucket 1's mean value is 3

	h.Sample(4)
	h.Sample(5)
	h.Sample(6)
	assert.Equal(uint64(3), h.buckets[2])
	assert.Equal(uint64(18), h.bucketSum(2)) // bucket 1's mean value is 3

	h.Sample(256)
	h.Sample(300)
	h.Sample(500)
	h.Sample(511)
	assert.Equal(uint64(4), h.buckets[8])
	assert.Equal(uint64(1536), h.bucketSum(8)) // bucket 1's mean value is 3

	assert.Equal(uint64(11), h.Samples())
	assert.Equal(uint64(1563), h.Sum())
	assert.Equal(uint64(142), h.Mean())
}

func TestHistogramAdd(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	h.Sample(1)  // sampled as 1.5
	h.Sample(2)  // sampled as 3
	h.Sample(10) // sampled as 12

	h2 := Histogram{}
	h2.Sample(3)          // sampled as 3
	h2.Sample(1073741854) // sampled as 1,610,612,736

	h.Add(h2)
	assert.Equal(uint64(5), h.Samples())
	assert.Equal(uint64(1610612755), h.Sum())
	assert.Equal(uint64(1610612755)/uint64(5), h.Mean())
}

func TestHistogramString(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	h.Sample(1)  // sampled as 1.5
	h.Sample(2)  // sampled as 3
	h.Sample(10) // sampled as 12
	h.Sample(3034030343)

	assert.Equal("Mean: 805306372, Sum: 3221225488, Samples: 4", h.String())

	th := NewTimeHistogram()
	th.Add(h)
	assert.Equal("Mean: 805.306372ms, Sum: 3.221225488s, Samples: 4", th.String())

	bh := NewByteHistogram()
	bh.Add(h)
	assert.Equal("Mean: 805 MB, Sum: 3.2 GB, Samples: 4", bh.String())
}
