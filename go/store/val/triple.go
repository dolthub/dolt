// Copyright 2022 Dolthub, Inc.
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

import "github.com/dolthub/dolt/go/store/pool"

const (
	tripleOffSz = int(uint16Size + uint16Size)
)

func NewTriple[V ~[]byte](pool pool.BuffPool, one, two, three V) (tri Triple[V]) {
	o1 := len(one)
	o2 := len(two) + o1
	end := len(three) + o2
	tri = pool.Get(uint64(end + tripleOffSz))

	// populate fields
	copy(tri, one)
	copy(tri[o1:], two)
	copy(tri[o2:], three)

	// populate offsets
	writeUint16(tri[end:end+2], uint16(o1))
	writeUint16(tri[end+2:], uint16(o2))
	return
}

type Triple[V ~[]byte] []byte

func (t Triple[V]) First() V {
	l := len(t)
	o1 := readUint16(t[l-4 : l-2])
	return V(t[:o1])
}

func (t Triple[V]) Second() V {
	l := len(t)
	o1 := readUint16(t[l-4 : l-2])
	o2 := readUint16(t[l-2 : l])
	return V(t[o1:o2])
}

func (t Triple[V]) Third() V {
	l := len(t)
	o2 := readUint16(t[l-2 : l])
	return V(t[o2 : l-4])
}
