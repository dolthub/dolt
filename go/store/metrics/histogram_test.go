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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

	h.Sample(2)
	h.Sample(3)
	assert.Equal(uint64(2), h.buckets[1])

	h.Sample(4)
	h.Sample(5)
	h.Sample(6)
	assert.Equal(uint64(3), h.buckets[2])

	h.Sample(256)
	h.Sample(300)
	h.Sample(500)
	h.Sample(511)
	assert.Equal(uint64(4), h.buckets[8])
	assert.Equal(uint64(11), h.Samples())
	assert.Equal(uint64(1589), h.Sum())
	assert.Equal(uint64(144), h.Mean())
}

func TestHistogramLarge(t *testing.T) {
	assert := assert.New(t)
	h := Histogram{}
	h.Sample(0xfffffffffffffe30)
	assert.Equal(uint64(1), h.Samples())
	assert.Equal(uint64(0xfffffffffffffe30), h.Sum())
}

func TestHistogramAdd(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	h.Sample(1)
	h.Sample(2)
	h.Sample(10)

	h2 := Histogram{}
	h2.Sample(3)
	h2.Sample(1073741854)

	h.Add(h2)
	assert.Equal(uint64(5), h.Samples())
	assert.Equal(uint64(1073741870), h.Sum())
	assert.Equal(uint64(1073741870)/uint64(5), h.Mean())
}

func TestHistogramString(t *testing.T) {
	assert := assert.New(t)

	h := Histogram{}
	h.Sample(1)
	h.Sample(2)
	h.Sample(10)
	h.Sample(3034030343)

	assert.Equal("Mean: 758507589, Sum: 3034030356, Samples: 4", h.String())

	th := NewTimeHistogram()
	th.Add(h)
	assert.Equal("Mean: 758.507589ms, Sum: 3.034030356s, Samples: 4", th.String())

	bh := NewByteHistogram()
	bh.Add(h)
	assert.Equal("Mean: 758 MB, Sum: 3.0 GB, Samples: 4", bh.String())
}

