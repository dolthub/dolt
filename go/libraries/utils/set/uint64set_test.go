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

package set

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUint64Set(t *testing.T) {
	initData := []uint64{0, 1, 2, 3}
	us := NewUint64Set(initData)

	// test .Size()
	assert.Equal(t, 4, us.Size())

	// test .Contains()
	for _, id := range initData {
		assert.True(t, us.Contains(id))
	}
	assert.False(t, us.Contains(5))


	// test .ContainsAll()
	assert.True(t, us.ContainsAll([]uint64{0, 1}))
	assert.False(t, us.ContainsAll([]uint64{0, 1, 2, 5}))

	// test .Add()
	us.Add(6)
	assert.True(t, us.Contains(6))
	assert.Equal(t, 5, us.Size())
	for _, id := range initData {
		assert.True(t, us.Contains(id))
	}
	assert.True(t, us.ContainsAll(append(initData, 6)))

}
