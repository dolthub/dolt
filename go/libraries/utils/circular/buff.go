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

type Buff[T any] struct {
	// The backing array.
	arr []T
	// Front() and Pop() refer to this element.
	front int
	// The number of elements in the buffer. [0, len(arr)).
	len int
}

func NewBuff[T any](initSz int) *Buff[T] {
	return &Buff[T]{
		arr: make([]T, initSz),
	}
}

// Returns the number of elements currently in Buff.
func (b *Buff[T]) Len() int {
	return b.len
}

func (b *Buff[T]) Cap() int {
	return cap(b.arr)
}

func (b *Buff[T]) At(i int) T {
	return *b.at(i)
}

func (b *Buff[T]) at(i int) *T {
	if i >= b.Len() {
		panic("At on Buff too small")
	}
	j := (b.front + i) % len(b.arr)
	return &b.arr[j]
}

func (b *Buff[T]) Front() T {
	return b.At(0)
}

func (b *Buff[T]) Pop() {
	if b.Len() == 0 {
		panic("Pop empty Buff")
	}
	// Don't leak entries...
	var empty T
	*b.at(0) = empty
	b.front = (b.front + 1) % len(b.arr)
	b.len -= 1
}

func (b *Buff[T]) ins() int {
	return (b.front + b.len) % len(b.arr)
}

func (b *Buff[T]) Push(t T) {
	if b.Len() == len(b.arr) {
		newarr := make([]T, len(b.arr)+len(b.arr))
		i := b.ins()
		if i > b.front {
			copy(newarr, b.arr[b.front:i])
		} else {
			first := b.arr[b.front:]
			copy(newarr, first)
			copy(newarr[len(first):], b.arr[:i])
		}
		b.arr = newarr
		b.front = 0
	}
	b.arr[b.ins()] = t
	b.len += 1
}
