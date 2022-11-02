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

package pool

type BuffPool interface {
	Get(size uint64) []byte
	GetSlices(size uint64) [][]byte
}

type buffPool byte

func NewBuffPool() BuffPool {
	return buffPool(0)
}

func (bp buffPool) Get(size uint64) []byte {
	return make([]byte, size)
}

func (bp buffPool) GetSlices(size uint64) [][]byte {
	return make([][]byte, size)
}
