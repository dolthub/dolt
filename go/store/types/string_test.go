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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringEquals(t *testing.T) {
	assert := assert.New(t)
	s1 := String("foo")
	s2 := String("foo")
	s3 := s2
	s4 := String("bar")
	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
	assert.True(s1.Equals(s3))
	assert.True(s3.Equals(s1))
	assert.False(s1.Equals(s4))
	assert.False(s4.Equals(s1))
}

func TestStringString(t *testing.T) {
	assert := assert.New(t)
	s1 := String("")
	s2 := String("foo")
	assert.Equal("", string(s1))
	assert.Equal("foo", string(s2))
}

func TestStringType(t *testing.T) {
	assert.True(t, mustType(TypeOf(String("hi"))).Equals(StringType))
}
