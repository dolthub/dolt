// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package hash

import (
	"testing"

	"github.com/attic-labs/noms/go/d"
	"github.com/stretchr/testify/assert"
)

func TestParseError(t *testing.T) {
	assert := assert.New(t)

	assertParseError := func(s string) {
		e := d.Try(func() { Parse(s) })
		_, ok := e.(d.WrappedError)
		assert.True(ok)
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

func TestGreater(t *testing.T) {
	assert := assert.New(t)

	r1 := Parse("00000000000000000000000000000001")
	r2 := Parse("00000000000000000000000000000002")

	assert.False(r1.Greater(r1))
	assert.False(r1.Greater(r2))
	assert.True(r2.Greater(r1))
	assert.False(r2.Greater(r2))

	r0 := Hash{}
	assert.False(r0.Greater(r0))
	assert.False(r0.Greater(r2))
	assert.True(r2.Greater(r0))
}
