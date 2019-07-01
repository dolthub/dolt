// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringEquals(t *testing.T) {
	assert := assert.New(t)
	s1 := String("foo")
	s2 := String("foo")
	s3 := s2
	s4 := String("bar")
	assert.True(s1.Equals(Format_7_18, s2))
	assert.True(s2.Equals(Format_7_18, s1))
	assert.True(s1.Equals(Format_7_18, s3))
	assert.True(s3.Equals(Format_7_18, s1))
	assert.False(s1.Equals(Format_7_18, s4))
	assert.False(s4.Equals(Format_7_18, s1))
}

func TestStringString(t *testing.T) {
	assert := assert.New(t)
	s1 := String("")
	s2 := String("foo")
	assert.Equal("", string(s1))
	assert.Equal("foo", string(s2))
}

func TestStringType(t *testing.T) {
	assert.True(t, TypeOf(String("hi")).Equals(Format_7_18, StringType))
}
