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

type Uint64Set struct {
	uints map[uint64]interface{}
}

func NewUint64Set(uints []uint64) *Uint64Set {
	s := &Uint64Set{make(map[uint64]interface{}, len(uints))}

	for _, b := range uints {
		s.uints[b] = emptyInstance
	}

	return s
}

func (us *Uint64Set) Contains(i uint64) bool {
	_, present := us.uints[i]
	return present
}

func (us *Uint64Set) ContainsAll(uints []uint64) bool {
	for _, b := range uints {
		if _, present := us.uints[b]; !present {
			return false
		}
	}

	return true
}

func (us *Uint64Set) Add(i uint64) {
	us.uints[i] = emptyInstance
}

func (us *Uint64Set) Size() int {
	return len(us.uints)
}

