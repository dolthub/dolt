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

package hash

import (
	"golang.org/x/crypto/blake2b"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"
)

func TestParseError(t *testing.T) {
	assert := assert.New(t)

	assertParseError := func(s string) {
		assert.Panics(func() {
			Parse(s)
		})
	}

	assertParseError("foo")

	// too few digits
	assertParseError("0000000000000000000000000000000")

	// too many digits
	assertParseError("000000000000000000000000000000000")

	// 'w' not valid base32
	assertParseError("00000000000000000000000000000000w")

	// no prefix
	assertParseError("sha1-00000000000000000000000000000000")
	assertParseError("sha2-00000000000000000000000000000000")

	r := Parse("00000000000000000000000000000000")
	assert.NotNil(r)
}

func TestMaybeParse(t *testing.T) {
	assert := assert.New(t)

	parse := func(s string, success bool) {
		r, ok := MaybeParse(s)
		assert.Equal(success, ok, "Expected success=%t for %s", success, s)
		if ok {
			assert.Equal(s, r.String())
		} else {
			assert.Equal(emptyHash, r)
		}
	}

	parse("00000000000000000000000000000000", true)
	parse("00000000000000000000000000000001", true)
	parse("", false)
	parse("adsfasdf", false)
	parse("sha2-00000000000000000000000000000000", false)
	parse("0000000000000000000000000000000w", false)
}

func TestEquals(t *testing.T) {
	assert := assert.New(t)

	r0 := Parse("00000000000000000000000000000000")
	r01 := Parse("00000000000000000000000000000000")
	r1 := Parse("00000000000000000000000000000001")

	assert.Equal(r0, r01)
	assert.Equal(r01, r0)
	assert.NotEqual(r0, r1)
	assert.NotEqual(r1, r0)
}

func TestString(t *testing.T) {
	s := "0123456789abcdefghijklmnopqrstuv"
	r := Parse(s)
	assert.Equal(t, s, r.String())
}

func TestOf(t *testing.T) {
	r := Of([]byte("abc"))
	assert.Equal(t, "rmnjb8cjc5tblj21ed4qs821649eduie", r.String())
}

func TestIsEmpty(t *testing.T) {
	r1 := Hash{}
	assert.True(t, r1.IsEmpty())

	r2 := Parse("00000000000000000000000000000000")
	assert.True(t, r2.IsEmpty())

	r3 := Parse("rmnjb8cjc5tblj21ed4qs821649eduie")
	assert.False(t, r3.IsEmpty())
}

func TestLess(t *testing.T) {
	assert := assert.New(t)

	r1 := Parse("00000000000000000000000000000001")
	r2 := Parse("00000000000000000000000000000002")

	assert.False(r1.Less(r1))
	assert.True(r1.Less(r2))
	assert.False(r2.Less(r1))
	assert.False(r2.Less(r2))

	r0 := Hash{}
	assert.False(r0.Less(r0))
	assert.True(r0.Less(r2))
	assert.False(r2.Less(r0))
}

func TestCompareGreater(t *testing.T) {
	assert := assert.New(t)

	r1 := Parse("00000000000000000000000000000001")
	r2 := Parse("00000000000000000000000000000002")

	assert.False(r1.Compare(r1) > 0)
	assert.False(r1.Compare(r2) > 0)
	assert.True(r2.Compare(r1) > 0)
	assert.False(r2.Compare(r2) > 0)

	r0 := Hash{}
	assert.False(r0.Compare(r0) > 0)
	assert.False(r0.Compare(r2) > 0)
	assert.True(r2.Compare(r0) > 0)
}

func init() {
	avg, std := 4096.0, 1024.0
	for i := range benchData {
		sz := int(rand.NormFloat64()*std + avg)
		benchData[i] = make([]byte, sz)
		rand.Read(benchData[i])
	}
}

var benchData [512][]byte

func BenchmarkSha512(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := i % len(benchData)
		_ = Of(benchData[j])
	}
}

func BenchmarkBlake2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := i % len(benchData)
		_ = blake2b.Sum256(benchData[j])
	}
}

func BenchmarkBlake3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := i % len(benchData)
		_ = blake3.Sum256(benchData[j])
	}
}

func BenchmarkXXHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		j := i % len(benchData)
		_ = xxh3.Hash128(benchData[j])
	}
}
