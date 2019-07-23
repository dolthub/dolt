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

type ByteSet struct {
	bytes map[byte]interface{}
}

func NewByteSet(bytes []byte) *ByteSet {
	s := &ByteSet{make(map[byte]interface{}, len(bytes))}

	for _, b := range bytes {
		s.bytes[b] = emptyInstance
	}

	return s
}

func (bs *ByteSet) Contains(b byte) bool {
	_, present := bs.bytes[b]
	return present
}

func (bs *ByteSet) ContainsAll(bytes []byte) bool {
	for _, b := range bytes {
		if _, present := bs.bytes[b]; !present {
			return false
		}
	}

	return true
}
