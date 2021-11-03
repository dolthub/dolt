// Copyright 2021 Dolthub, Inc.
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

package val

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/pool"
)

var maskPool = pool.NewBuffPool()

func TestMemberSet(t *testing.T) {
	for i := 1; i < 100; i++ {
		name := fmt.Sprintf("test member set sizeOf %d", i)
		t.Run(name, func(t *testing.T) {
			testMemberSet(t, i)
			testCountPrefix(t, i)
			testCountSuffix(t, i)
		})
	}
}

func testMemberSet(t *testing.T, count int) {
	memSet := makeMemberMask(maskPool, count)

	for i := 0; i < count; i++ {
		assert.False(t, memSet.present(i))
		assert.Equal(t, i, memSet.count())
		memSet.set(i)
		assert.True(t, memSet.present(i))
		assert.Equal(t, i+1, memSet.count())
	}

	for i := count - 1; i >= 0; i-- {
		assert.True(t, memSet.present(i))
		assert.Equal(t, i+1, memSet.count())
		memSet.unset(i)
		assert.False(t, memSet.present(i))
		assert.Equal(t, i, memSet.count())
	}

	expected := setRandom(memSet, count)
	assert.Equal(t, expected, memSet.count())
}

func testCountPrefix(t *testing.T, count int) {
	memSet := makeMemberMask(maskPool, count)

	for i := 0; i < count; i++ {
		assert.Equal(t, i, memSet.countPrefix(i))
		memSet.set(i)
		assert.Equal(t, i+1, memSet.countPrefix(i))
	}

	expected := setRandom(memSet, count)
	assert.Equal(t, expected, memSet.countPrefix(count-1))
}

func testCountSuffix(t *testing.T, count int) {
	memSet := makeMemberMask(maskPool, count)

	c := 0
	for i := count - 1; i >= 0; i-- {
		assert.Equal(t, c, memSet.countSuffix(i))
		memSet.set(i)
		c++
		assert.Equal(t, c, memSet.countSuffix(i))
	}

	expected := setRandom(memSet, count)
	assert.Equal(t, expected, memSet.countSuffix(0))
}

func setRandom(ms memberMask, len int) (count int) {

	for i := 0; i < len; i++ {
		if rand.Int()%2 == 1 {
			ms.set(i)
			count++
		} else {
			ms.unset(i)
		}
	}
	return
}

func TestMemberSetUtils(t *testing.T) {
	assert.Equal(t, byte(0x01), prefixMask(0))
	assert.Equal(t, byte(0x03), prefixMask(1))
	assert.Equal(t, byte(0x07), prefixMask(2))
	assert.Equal(t, byte(0x0f), prefixMask(3))
	assert.Equal(t, byte(0x1f), prefixMask(4))
	assert.Equal(t, byte(0x3f), prefixMask(5))
	assert.Equal(t, byte(0x7f), prefixMask(6))
	assert.Equal(t, byte(0xff), prefixMask(7))

	assert.Equal(t, byte(0xff), suffixMask(0))
	assert.Equal(t, byte(0xfe), suffixMask(1))
	assert.Equal(t, byte(0xfc), suffixMask(2))
	assert.Equal(t, byte(0xf8), suffixMask(3))
	assert.Equal(t, byte(0xf0), suffixMask(4))
	assert.Equal(t, byte(0xe0), suffixMask(5))
	assert.Equal(t, byte(0xc0), suffixMask(6))
	assert.Equal(t, byte(0x80), suffixMask(7))

}
