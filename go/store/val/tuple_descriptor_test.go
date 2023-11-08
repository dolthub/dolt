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

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestTupleDescriptorSize(t *testing.T) {
	sz := unsafe.Sizeof(TupleDesc{})
	assert.Equal(t, 64, int(sz))
}

func TestTupleDescriptorAddressTypes(t *testing.T) {
	types := []Type{
		{Enc: BytesAddrEnc},
		{Enc: CommitAddrEnc},
		{Enc: StringAddrEnc},
		{Enc: JSONAddrEnc},
		{Enc: GeomAddrEnc},
	}
	td := NewTupleDescriptor(types...)

	assert.Equal(t, 5, td.AddressFieldCount())
	IterAddressFields(td, func(i int, typ Type) {
		assert.Equal(t, types[i], typ)
	})
}
