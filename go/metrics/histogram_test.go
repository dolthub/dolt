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

func TestHistogramReport(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	h.Sample(1) // sampled as 1.5
	assert.Equal("----------------------------------------------------------------------------------------------------> 1: (1)", h.Report())

	h.Sample(1 << 62)
	assert.Equal(`----------------------------------------------------------------------------------------------------> 1: (1)
> 2: (0)
> 4: (0)
> 8: (0)
> 16: (0)
> 32: (0)
> 64: (0)
> 128: (0)
> 256: (0)
> 512: (0)
> 1024: (0)
> 2048: (0)
> 4096: (0)
> 8192: (0)
> 16384: (0)
> 32768: (0)
> 65536: (0)
> 131072: (0)
> 262144: (0)
> 524288: (0)
> 1048576: (0)
> 2097152: (0)
> 4194304: (0)
> 8388608: (0)
> 16777216: (0)
> 33554432: (0)
> 67108864: (0)
> 134217728: (0)
> 268435456: (0)
> 536870912: (0)
> 1073741824: (0)
> 2147483648: (0)
> 4294967296: (0)
> 8589934592: (0)
> 17179869184: (0)
> 34359738368: (0)
> 68719476736: (0)
> 137438953472: (0)
> 274877906944: (0)
> 549755813888: (0)
> 1099511627776: (0)
> 2199023255552: (0)
> 4398046511104: (0)
> 8796093022208: (0)
> 17592186044416: (0)
> 35184372088832: (0)
> 70368744177664: (0)
> 140737488355328: (0)
> 281474976710656: (0)
> 562949953421312: (0)
> 1125899906842624: (0)
> 2251799813685248: (0)
> 4503599627370496: (0)
> 9007199254740992: (0)
> 18014398509481984: (0)
> 36028797018963968: (0)
> 72057594037927936: (0)
> 144115188075855872: (0)
> 288230376151711744: (0)
> 576460752303423488: (0)
> 1152921504606846976: (0)
> 2305843009213693952: (0)
----------------------------------------------------------------------------------------------------> 4611686018427387904: (1)`, h.Report())

	h = Histogram{}
	h.Sample(4)
	h.Sample(8)

	assert.Equal(`----------------------------------------------------------------------------------------------------> 4: (1)
----------------------------------------------------------------------------------------------------> 8: (1)`, h.Report())
}
