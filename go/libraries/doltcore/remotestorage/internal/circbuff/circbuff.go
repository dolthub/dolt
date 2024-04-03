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

package circbuff

type Buff[T any] struct {
	// The backing array.
	arr []T
	// Front() and Pop() refer to this element.
	front int
	// Push() pushes here.
	back int
}

func NewBuff[T any](initSz int) Buff[T] {
	return Buff[T]{
		arr: make([]T, initSz),
	}
}

// Returns the number of elements currently in Buff.
func (b *Buff[T]) Len() int {
	if b.back == b.front {
		return 0
	} else if b.back > b.front {
		return b.back - b.front
	} else {
		return b.back - b.front + len(b.arr)
	}
}

func (b *Buff[T]) Front() T {
	if b.Len() == 0 {
		panic("Front empty Buff")
	}
	return b.arr[b.front]
}

func (b *Buff[T]) Pop() {
	if b.Len() == 0 {
		panic("Pop empty Buff")
	}
	b.front = (b.front + 1) % len(b.arr)
}

func (b *Buff[T]) Push(t T) {
	if b.Len() == len(b.arr)-1 {
		newarr := make([]T, len(b.arr)+len(b.arr))
		var newback int
		if b.back > b.front {
			copy(newarr, b.arr[b.front:b.back])
			newback = b.back - b.front
		} else {
			first := b.arr[b.front:]
			copy(newarr, first)
			copy(newarr[len(first):], b.arr[:b.back])
			newback = len(first) + b.back
		}
		b.arr = newarr
		b.front = 0
		b.back = newback
	}
	b.arr[b.back] = t
	b.back = (b.back + 1) % len(b.arr)
}
