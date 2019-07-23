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

package random

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testReader byte

func (r *testReader) Read(dest []byte) (int, error) {
	for i := 0; i < len(dest); i++ {
		dest[i] = byte(*r)
	}
	return len(dest), nil
}

func TestBasic(t *testing.T) {
	assert := assert.New(t)

	func() {
		var r testReader
		oldReader := reader
		reader = &r
		defer func() {
			reader = oldReader
		}()

		r = testReader(byte(0x00))
		assert.Equal("00000000000000000000000000000000", Id())
		r = testReader(byte(0x01))
		assert.Equal("01010101010101010101010101010101", Id())
		r = testReader(byte(0xFF))
		assert.Equal("ffffffffffffffffffffffffffffffff", Id())
	}()

	one := Id()
	two := Id()
	assert.NotEqual(one, two)
}
