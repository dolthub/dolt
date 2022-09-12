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
	}
	td := NewTupleDescriptor(types...)

	assert.Equal(t, 4, td.AddressFieldCount())
	IterAddressFields(td, func(i int, typ Type) {
		assert.Equal(t, types[i], typ)
	})
}

func TestTupleCanCompare(t *testing.T) {
	tests := []struct {
		Name       string
		From       []Type
		To         []Type
		Comparable bool
	}{
		{
			Name:       "empty",
			From:       nil,
			To:         nil,
			Comparable: true,
		},
		{
			Name:       "modified a column",
			From:       []Type{{Enc: Int16Enc, Nullable: true}},
			To:         []Type{{Enc: Int16Enc, Nullable: true}},
			Comparable: true,
		},
		{
			Name:       "added nullable columns",
			From:       nil,
			To:         []Type{{Enc: Int16Enc, Nullable: true}, {Enc: Int16Enc, Nullable: true}},
			Comparable: true,
		},
		{
			Name:       "added non-null columns",
			From:       nil,
			To:         []Type{{Enc: Int16Enc, Nullable: true}, {Enc: Int16Enc, Nullable: false}},
			Comparable: false,
		},
		{
			Name:       "modified a column",
			From:       []Type{{Enc: Int16Enc, Nullable: true}},
			To:         []Type{{Enc: Int16Enc, Nullable: false}},
			Comparable: false,
		},
		{
			Name:       "dropped a column",
			From:       []Type{{Enc: Int16Enc, Nullable: true}},
			To:         nil,
			Comparable: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			fd := NewTupleDescriptor(tc.From...)
			td := NewTupleDescriptor(tc.To...)
			assert.Equal(t, tc.Comparable, CanCompareTuples(fd, td))
		})
	}
}
