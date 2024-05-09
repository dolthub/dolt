// Copyright 2024 Dolthub, Inc.
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

package circular

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuff(t *testing.T) {
	t.Run("FixedSize8", func(t *testing.T) {
		b := NewBuff[int](8)
		assert.Equal(t, 0, b.Len())
		b.Push(1)
		assert.Equal(t, 1, b.Len())
		assert.Equal(t, 1, b.Front())
		b.Pop()
		assert.Equal(t, 0, b.Len())

		b.Push(8)
		assert.Equal(t, 1, b.Len())
		assert.Equal(t, 8, b.Front())

		for i := 0; i < 8; i++ {
			b.Push(2)
			assert.Equal(t, 2, b.Len())
			b.Push(3)
			assert.Equal(t, 3, b.Len())
			b.Push(4)
			assert.Equal(t, 4, b.Len())
			b.Push(5)
			assert.Equal(t, 5, b.Len())
			b.Push(6)
			assert.Equal(t, 6, b.Len())
			b.Push(7)
			assert.Equal(t, 7, b.Len())
			b.Push(8)
			assert.Equal(t, 8, b.Len())

			assert.Equal(t, 8, b.At(0))
			assert.Equal(t, 2, b.At(1))
			assert.Equal(t, 3, b.At(2))
			assert.Equal(t, 4, b.At(3))
			assert.Equal(t, 5, b.At(4))
			assert.Equal(t, 6, b.At(5))
			assert.Equal(t, 7, b.At(6))
			assert.Equal(t, 8, b.At(7))

			assert.Equal(t, 8, b.Front())
			b.Pop()
			assert.Equal(t, 7, b.Len())
			assert.Equal(t, 2, b.Front())
			b.Pop()
			assert.Equal(t, 6, b.Len())
			assert.Equal(t, 3, b.Front())
			b.Pop()
			assert.Equal(t, 5, b.Len())
			assert.Equal(t, 4, b.Front())
			b.Pop()
			assert.Equal(t, 4, b.Len())
			assert.Equal(t, 5, b.Front())
			b.Pop()
			assert.Equal(t, 3, b.Len())
			assert.Equal(t, 6, b.Front())
			b.Pop()
			assert.Equal(t, 2, b.Len())
			assert.Equal(t, 7, b.Front())
			b.Pop()
			assert.Equal(t, 1, b.Len())
			assert.Equal(t, 8, b.Front())
		}

		assert.Equal(t, 1, b.Len())
		assert.Equal(t, 8, b.Front())
		b.Pop()
		assert.Equal(t, 0, b.Len())
	})
	t.Run("Growing", func(t *testing.T) {
		b := NewBuff[int](2)
		for i := 0; i < 16; i++ {
			b.Push(i)
			assert.Equal(t, i+1, b.Len())
			assert.Equal(t, 0, b.Front())
		}
		for i := 0; i < 16; i++ {
			assert.Equal(t, i, b.Front())
			b.Pop()
			b.Push(i + 16)
			assert.Equal(t, 16, b.Len())
		}
		for i := 0; i < 16; i++ {
			assert.Equal(t, 16+i, b.Len())
			b.Push((i * 2) + 32)
			b.Push((i * 2) + 33)
			assert.Equal(t, i+16, b.Front())
			b.Pop()
		}
		for i := 0; i < 16; i++ {
			assert.Equal(t, (i*2)+32, b.Front())
			b.Pop()
			assert.Equal(t, (i*2)+33, b.Front())
			b.Pop()
		}
		assert.Equal(t, 0, b.Len())
	})
	t.Run("Panics", func(t *testing.T) {
		b := NewBuff[int](2)
		t.Run("FrontEmpty", func(t *testing.T) {
			assert.Panics(t, func() {
				b.Front()
			})
		})
		t.Run("PopEmpty", func(t *testing.T) {
			assert.Panics(t, func() {
				b.Pop()
			})
		})
	})
}
