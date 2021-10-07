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

package fish

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindLargerBucket(t *testing.T) {
	tests := []struct {
		requested uint32
		expBucket uint32
	}{
		{requested: 63, expBucket: 0},
		{requested: 64, expBucket: 0},
		{requested: 65, expBucket: 1},
		{requested: 16383, expBucket: 8},
		{requested: 16384, expBucket: 8},
		{requested: 16385, expBucket: 9},
		{requested: 32768, expBucket: 9},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			act := findLargerBucket(test.requested)
			assert.Equal(t, test.expBucket, act)

			expSize := minimumSize << test.expBucket
			buf := NewLadder().Get(test.requested)
			assert.Equal(t, expSize, uint32(len(buf)))
		})
	}
}

func TestFindSmallerBucket(t *testing.T) {
	tests := []struct {
		requested uint32
		expBucket uint32
	}{
		//{requested: 63, expBucket: 0},
		{requested: 64, expBucket: 0},
		{requested: 127, expBucket: 0},
		{requested: 128, expBucket: 1},
		{requested: 129, expBucket: 1},
		{requested: 32768, expBucket: 9},
		{requested: 32769, expBucket: 9},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
			act := findSmallerBucket(test.requested)
			assert.Equal(t, test.expBucket, act)
		})
	}
}
